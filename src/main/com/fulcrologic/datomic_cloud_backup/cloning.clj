(ns com.fulcrologic.datomic-cloud-backup.cloning
  "Copy a database with history via transaction logs. Based on code from Cognitect."
  (:require
    [com.fulcrologic.guardrails.core :refer [>defn => ?]]
    [clojure.core.async :as async]
    [clojure.pprint :refer [pprint]]
    [clojure.set :as set]
    [com.fulcrologic.datomic-cloud-backup.protocols :as dcbp]
    [datomic.client.api :as d]
    [datomic.client.api.protocols :as dp]
    [taoensso.timbre :as log]
    [taoensso.truss :refer [have]]
    [taoensso.tufte :as tufte :refer [profile p]]
    [clojure.spec.alpha :as s])
  (:import (clojure.lang ExceptionInfo)
           (java.util Date)))

(s/def ::db #(satisfies? dp/Db %))
(s/def ::datom #(int? (:e %)))
(s/def ::db-name (s/or :string string? :k keyword?))
(s/def ::t int?)
(s/def ::data any?)
(s/def ::txn (s/keys :req-un [::t ::data]))
(s/def ::connection #(satisfies? dp/Connection %))
(s/def ::e (s/or :tmp string? :real int? :ident keyword? :special #{:db.part/db}))
(s/def ::a (s/or :id int? :ident keyword? :tmpid string?))
(s/def ::v any?)
(s/def ::op #{:db/add :db/retract})
(s/def ::casop #{:db/cas})
(s/def ::tx int?)
(s/def ::added boolean?)
(s/def ::datom-map (s/keys :req-un [::e ::a ::v ::tx ::added]))
(s/def ::txn-op (s/or
                  :cas (s/tuple ::casop ::e ::a ::v ::v)
                  :add-or-retract (s/tuple ::op ::e ::a ::v)))
(s/def ::to-one? ifn?)
(s/def ::id->attr ifn?)
(s/def ::source-refs (s/coll-of int? :kind set?))
(s/def ::rewrite (? (s/map-of keyword? fn?)))
(s/def ::blacklist (? (s/every keyword? :kind set?)))

(>defn all-refs
  "Return a set of all :db.type/ref entity IDs from the provided DB"
  [db]
  [::db => (s/every int? :kind set?)]
  (p `all-refs
    (into #{}
      (map first
        (d/q '[:find ?e
               :in $
               :where
               [?e :db/valueType :db.type/ref]] db)))))

(>defn datom->map
  [{:keys [e a v tx added]}]
  [::datom => ::datom-map]
  {:e     e
   :a     a
   :v     v
   :tx    tx
   :added added})

(defn mapify-datoms
  [datoms]
  (mapv datom->map datoms))

(>defn id->attr
  [db]
  [::db => (s/map-of int? keyword?)]
  (into {}
    (d/q
      '[:find ?e ?a
        :where
        [?e :db/ident ?a]]
      db)))

(defn backup-segment!
  "Takes an explicit transaction range, and copies it from the database to
   the `backup-store`.  `end-t` is exclusive.

   Returns the real {:start-t a :end-t b} that was actually stored, which may be nil
   if nothing was stored.

   "
  [database-name source-conn backup-store start-t end-t]
  (let [db                 (d/db source-conn)
        ref-attr-ids       (all-refs db)
        tx-group           (d/tx-range source-conn {:start start-t :end end-t :limit -1})
        serializable-group (mapv #(update % :data mapify-datoms) tx-group)
        actual-start       (-> (vec tx-group) first :t)
        actual-end         (-> tx-group last :t)]
    (if (and actual-start actual-end)
      (do
        (dcbp/save-transactions! backup-store database-name
          {:refs         ref-attr-ids
           :id->attr     (id->attr (d/as-of db #inst "2000-01-01"))
           :transactions serializable-group
           :start-t      actual-start
           :end-t        actual-end})
        (log/infof "Backed up transactions for %s t %d through %d (inclusive)." database-name actual-start actual-end)
        {:start-t actual-start :end-t actual-end})
      {})))

(defn backup-next-segment!
  "
  Computes the next transaction range that is missing from `backup-store` and writes up to
  `max-txns` to the store.

  Returns the number of transactions written (if that is `max-txns`, then
  there may be more transaction that need backing up). This call has to analyze the names of all segments, and
  therefore should not be called too frequently if that is expensive on your backup-store.

  See also `backup-segment!` if you want to implement your own segmentation strategy."
  [database-name source-conn backup-store max-txns]
  (let [{:keys [end-t]
         :or   {end-t 0}} (dcbp/last-segment-info backup-store database-name)
        start-t (inc end-t)
        last-t  (+ start-t max-txns)
        {:keys [start-t end-t]} (backup-segment! database-name source-conn backup-store start-t last-t)]
    (if (and start-t end-t)
      (do
        (log/infof "Backed up transactions for %s t %d through %d (inclusive)." database-name start-t end-t)
        (inc (- end-t start-t)))
      0)))

(defn backup!
  "Run a backup.

  * dbname - What to call this backup in the store. Does not have to match the actual database name on the connection
  * connection - The connection of the database to back up
  * store - The BackupStore to write to
  * options:
    * :txns-per-segment - The number of transactions to try to put in each segment of the backup. Defaults to 10,000
    * :starting-segment - The starting segment of the backup (default 0). Use this to continue a previous backup. The starting-segment
      times the txns-per-segment determines the transaction on which the backup will start. Defaults to 0.
    * :parallel? - Default true. Run segment backups in parallel. WARNING: This can stress the DynamoDB layer. The backup
      utility will retry on capacity exceptions, but beware that your application could also receive capacity exceptions.

  This function retries if there is any kind of transitional failure on any segment. A persistent failure can cause
  the backup to terminate early with an exception.
  "
  [dbname connection store {:keys [txns-per-segment
                                   starting-segment
                                   parallel?]
                            :or   {txns-per-segment 10000
                                   starting-segment 0
                                   parallel?        true}}]
  (let [db       (d/db connection)
        Map      (if parallel? pmap map)
        segments (inc (int (/ (:t db) txns-per-segment)))
        failed?  (atom false)]
    (doall
      (Map
        (fn [segment-number]
          (when-not @failed?
            (let [start (* segment-number txns-per-segment)
                  end   (+ start txns-per-segment)]
              (loop [attempt 0]
                (let [ok?    (atom false)
                      result (atom nil)]
                  (try
                    (reset! result (backup-segment! dbname connection store start end))
                    (reset! ok? true)
                    (catch Exception e
                      (log/warn e "Backup step failed. Retrying" attempt)))
                  (cond
                    (> attempt 2) (do
                                    (reset! failed? true)
                                    (log/error "BACKUP FAILED. Too many attempts on segment" segment-number [start end])
                                    (throw (ex-info "BACKUP FAILED." {:segment-number segment-number
                                                                      :start-t        start
                                                                      :end-t          end})))
                    (not @ok?) (do
                                 (Thread/sleep 1000)
                                 (recur (inc attempt)))
                    :else @result))))))
        (range starting-segment segments)))))

(>defn rewrite-and-filter-txn
  [{:keys [to-one? id->attr rewrite blacklist]} transaction]
  [(s/keys
     :req-un [::to-one? ::id->attr]
     :opt-un [::rewrite ::blacklist])
   (s/coll-of ::txn-op :kind vector?) => (s/coll-of ::txn-op :kind vector?)]
  (p `rewrite-and-filter-txn
    (->> transaction
      (sort-by #(not= :db/add (first %)))
      (reduce
        (fn [{:keys [adds] :as acc} [op e a v :as original]]
          (let [added    (= :db/add op)
                v        (if-let [rewrite (and added (get rewrite a))]
                           (rewrite a v)
                           v)
                retract? (= :db/retract op)
                noop?    (and retract? (contains? adds a) to-one? (to-one? a))]
            ;; This is necessary because rewrite can cause the add/retract to match, which will cause
            ;; a datom conflict.
            (cond
              noop? acc
              added (-> acc
                      (update :adds conj a)
                      (update :result conj [op e a v]))
              retract? (update acc :result conj [op e a v])
              :else (update acc :result conj original))))
        {:adds #{} :result []})
      :result
      (remove #(contains? blacklist (nth % 2)))
      vec)))

(>defn tx-time
  "Returns the transaction time of an entry from a tx-entry."
  [{:keys [data]}]
  [::txn => inst?]
  (let [tx-id (:tx (first data))
        tm    (:v (first (filter #(and
                                    (= tx-id (:e %))
                                    (inst? (:v %))) data)))]
    (or tm #inst "1970-01-01")))

(defn -load-transactions
  "Protocols don't mock well. This is just a wrapper to facilitate a testing scenario."
  [backup-store dbname start-t]
  (dcbp/load-transaction-group backup-store dbname start-t))

(defn- to-many? [db attr]
  (try
    (= :db.cardinality/many
      (ffirst
        (d/q
          '[:find ?c
            :in $ ?attr
            :where
            [?attr :db/cardinality ?card]
            [?card :db/ident ?c]]
          db attr)))
    (catch Exception _
      true)))

(defn- to-one? [db attr] (not (to-many? db attr)))

(defn tempid-tracking-schema-txns
  [connection]
  (let [time (-> (d/tx-range connection {:start 1 :limit -1}) last tx-time)
        tma  (Date. (long (+ 1000 (inst-ms time))))
        tmb  (Date. (long (+ 2000 (inst-ms time))))]
    [[{:db/id        "datomic.tx"
       :db/txInstant tma}
      {:db/ident       ::original-id
       :db/cardinality :db.cardinality/one
       :db/valueType   :db.type/long
       :db/doc         "Tracks the original :db/id of the entity in the source database."}
      {:db/ident       ::last-source-transaction
       :db/noHistory   true
       :db/cardinality :db.cardinality/one
       :db/valueType   :db.type/long
       :db/doc         "The last *source* transaction that was successfully processed against this database."}]
     [{:db/id        "datomic.tx"
       :db/txInstant tmb}
      [:db/add ::last-source-transaction ::last-source-transaction 0]]]))

(>defn ensure-restore-schema!
  [connection]
  [::connection => boolean?]
  (let [db          (d/db connection)
        has-schema? (boolean
                      (first
                        (seq
                          (d/datoms db {:index      :avet
                                        :components [:db/ident ::original-id]
                                        :limit      1}))))]
    (when-not has-schema?
      (log/info "Adding schema to track :db/id remappings.")
      (doseq [txn (tempid-tracking-schema-txns connection)]
        (d/transact connection {:tx-data txn})))
    true))

(>defn resolve-id
  "Finds the new database's :db/id for the given :db/id, or returns a stringified version of the ID for use as a new
   tempid."
  [{:keys [db tx-id id->attr]} old-id]
  [(s/keys :ref-un [::db ::tx-id ::id->attr]) (s/or :id int? :ident keyword?) => ::e]
  (p `resolve-id
    (let [old-id (get id->attr old-id old-id)]
      (cond
        (= tx-id old-id) "datomic.tx"
        (keyword? old-id) old-id
        :else (or
                (let [matching-datoms (seq
                                        (d/datoms db {:index      :avet
                                                      :components [::original-id old-id]}))]
                  (when (> (count matching-datoms) 1)
                    (throw (ex-info "ID Resolution failed! Two entities share the same original ID!" {:original-id old-id})))
                  (-> matching-datoms first :e))
                (str old-id))))))

(>defn bookkeeping-txn
  "Generates a transaction that includes a CAS that verifies the current basis t of the database is the intended
   target, and the adds ::original-id datoms for every tempid necessary in the transaction to track the original
   source (as in source database) entity. This allows future transactions to simply look up the current ID of an
   original ID during ID resolution."
  [{:keys [db] :as env} {:keys [t data]}]
  [(s/keys :req-un [::db]) ::txn => (s/coll-of ::txn-op :kind vector?)]
  (p `bookkeeping-txn
    (let [unique-ids                   (into #{} (map :e) (vec data))
          current-t                    (p ::current-t (ffirst (d/q '[:find ?t :where [::last-source-transaction ::last-source-transaction ?t]] db)))
          tx-id                        (-> data first :tx)
          env                          (assoc env :tx-id tx-id)
          ensure-restoring-correct-txn [:db/cas ::last-source-transaction ::last-source-transaction
                                        (if (zero? current-t) 0 (dec t)) t]]
      (reduce
        (fn [acc id]
          (let [resolved-id (if (= id tx-id)
                              "datomic.tx"
                              (resolve-id env id))]
            (if (string? resolved-id)
              (conj acc [:db/add resolved-id ::original-id id])
              acc)))
        [ensure-restoring-correct-txn]
        unique-ids))))

(>defn resolved-txn
  "This function rewrites an incoming transaction log `tx-entry` into
   a Datomic transaction that remaps the necessary IDs to maintain referential integrity.

   The transaction will include what `bookkeeping-txn` outputs, and all of the transaction data as
   new transaction operations that have had their IDs remapped according to what has already been restored (e and a, and
   v when a is a ref).

   This function requires the current target db (for resolving original IDs), `id->attr` for finding the mappings from
   the base datomic schema in the old database to the new one, and a set of attribute db ids (in the source db)
   that represent ref attributes in the source database (`source-refs`).
   "
  [{:keys [db id->attr source-refs] :as env} {:keys [t data] :as tx-entry}]
  [(s/keys :req-un [::db ::id->attr ::source-refs]) ::txn => (s/coll-of ::txn-op :kind vector?)]
  (p `resolved-txn
    (let [tx-id (-> data first :tx)
          tm    (tx-time tx-entry)
          env   (assoc env :tx-id tx-id)
          k->id (into {}
                  (keep
                    (fn [[eid a k? _tx added? :as datom]]
                      (when (and added?
                              (= (id->attr a) :db/ident)
                              (keyword? k?))
                        [k? eid])))
                  data)
          data  (mapv
                  (fn [m]
                    (update m :v (fn [v]
                                   (if (vector? v)
                                     (mapv (fn [k] (get k->id k k)) v)
                                     v))))
                  (mapify-datoms data))]
      (log/info "In-transaction schema map:" k->id)
      (if (< (compare tm #inst "2000-01-01") 0)
        ;; Record that we has an empty transaction. The timestamp here is a bit of a pain, since I'm just
        ;; guessing...but it looks like datomic internals set their timestamp to the UNIX epoch
        (let [tx-time (Date. (long (+ t (inst-ms #inst "1970-01-02"))))]
          [[:db/cas ::last-source-transaction ::last-source-transaction (dec t) t]
           [:db/add "datomic.tx" :db/txInstant tx-time]])
        (into (bookkeeping-txn env tx-entry)
          (map
            (fn [{:keys      [e v added]
                  original-a :a}]
              (let [op (if added :db/add :db/retract)
                    e  (if (= e tx-id)
                         "datomic.tx"
                         (resolve-id env e))
                    a  (resolve-id env original-a)
                    v  (cond
                         (= v tx-id) "datomic.tx"
                         (and (keyword? a) (= "db" (namespace a)) (int? v)) (resolve-id env v)
                         (= a :db.install/attribute) (str v)
                         (contains? source-refs original-a) (resolve-id env v)
                         :else v)]
                [op e a v])))
          data)))))

(defn find-segment-start-t
  "When resuming a backup the last t in the database won't likely be on a segment boundary. This scans the real segments
   to find the correct start-t for loading."
  [store dbname desired-start-t]
  (if (= desired-start-t (:start-t (dcbp/last-segment-info store dbname)))
    desired-start-t
    (let [saved-segments (dcbp/saved-segment-info store dbname)]
      (->> saved-segments
        (filter (fn [{:keys [start-t end-t]}]
                  (<= start-t desired-start-t end-t)))
        first
        :start-t))))

(>defn prune-tempids-as-values [target-refs datoms]
  [(s/coll-of int? :kind set?) (s/coll-of ::txn-op) => (s/coll-of ::txn-op)]
  (p `prune-tempids-as-values
    (let [tempids-as-e (into #{}
                         (comp
                           (filter #(not= :db/cas (first %)))
                           (map second)
                           (filter string?))
                         datoms)
          ref?         (fn [a] (contains? target-refs a))]
      (reduce
        (fn [acc [op e a v :as txnop]]
          (if (and (not= op :db/cas) (ref? a))
            (if (or (int? v) (contains? tempids-as-e v))
              (conj acc txnop)
              (do
                (log/warn "Dropped transaction operation" txnop
                  "because v was a tempid that was only used as a value in the transaction:" datoms)
                acc))
            (conj acc txnop)))
        []
        datoms))))

(>defn restore-segment!
  "Restore the next segment of a backup. Auto-detects (from the target database) where to resume.

  Returns one of three possible values:

  * :restored-segment - Real work was done to restore more data.
  * :nothing-new-available - The backup is restored to the current end. There was nothing to do.
  * :transaction-failed! - The restore tried to restore data, but something went wrong. Check the logs. MAY work
    if re-attempted (e.g. if it was a temporary Datomic outage)
  * :partial-segment - The restore found a segment that was supposed to have more data in it than it really had.

  The arguments are:

  * source-database-name - The name used for this backup.
  * target-conn - The connection of the target db on which to restore.
  * backup-store - A durable store that was used to save the database
  * options - A map of other options
  ** :blacklist - A set of keywords (attributes) to elide from all transactions.
  ** :rewrite - A map from attribute keyword to a `(fn [attr value] new-value)` that can be used to obfuscate the original value to a new one.
  "
  [source-database-name target-conn backup-store {:keys [blacklist rewrite]
                                                  :or   {blacklist #{}}}]
  [::db-name ::connection ::dcbp/store (s/keys :opt-un [::blacklist ::rewrite]) => #{:restored-segment
                                                                                     :nothing-new-available
                                                                                     :transaction-failed!}]
  (let [current-db             (d/db target-conn)
        last-restored-t        (or
                                 (::last-source-transaction (d/pull current-db [::last-source-transaction] ::last-source-transaction))
                                 0)
        desired-start-t        (if (and last-restored-t (pos? last-restored-t)) (inc last-restored-t) 1)
        {last-available-start-t :start-t} (dcbp/last-segment-info backup-store source-database-name)
        last-available-start-t (if (and last-available-start-t (zero? last-available-start-t)) 1 last-available-start-t)]
    (if (or (nil? last-available-start-t) (< last-available-start-t desired-start-t))
      :nothing-new-available
      (let
        [segment-start-t (if (< desired-start-t 2) desired-start-t (find-segment-start-t backup-store source-database-name desired-start-t))
         {:keys [refs id->attr transactions] :as tgi} (-load-transactions backup-store source-database-name segment-start-t)
         result          (atom :restored-segment)]
        (when (< segment-start-t 2) (ensure-restore-schema! target-conn))
        (log/infof "Restoring %s segment %d starting at %d." source-database-name segment-start-t desired-start-t)
        (log/infof "There are %d transactions in the segment found." (count transactions))
        (if (<= (+ segment-start-t (count transactions)) desired-start-t)
          (do
            (log/error "Transaction group does NOT have the starting point needed!")
            :partial-segment)
          (doseq [{:keys [t] :as tx-entry} transactions
                  :when (and (> t last-restored-t) (= @result :restored-segment))]
            (let [db           (d/db target-conn)
                  target-refs  (all-refs db)
                  resolved-txn (resolved-txn {:db          db
                                              :id->attr    id->attr
                                              :source-refs refs} tx-entry)
                  pruned-txn   (prune-tempids-as-values target-refs resolved-txn)
                  final-txn    (rewrite-and-filter-txn {:to-one?   (partial to-one? (d/db target-conn))
                                                        :id->attr  id->attr
                                                        :blacklist blacklist
                                                        :rewrite   rewrite} pruned-txn)]
              (try
                (when (empty? final-txn)
                  (throw (ex-info "Incorrect transaction didn't record restore (empty!)" {})))
                (log/debug "Committing transaction" t)
                (d/transact target-conn {:tx-data final-txn
                                         :timeout 10000000})
                (log/debug "Finished transaction" t)
                (reset! result :restored-segment)
                (catch Throwable e
                  (reset! result :transaction-failed!)
                  (log/error e "Restore transaction failed!"
                    {:db      source-database-name
                     :message (ex-message e)
                     :data    (ex-data e)
                     :t       t}))))))
        @result))))

(defn restore!!
  "Restore as much of the database as possible. This is an interruptible call (you can reboot and nothing will be
   harmed). This function does as much pipelining and optimization as possible to try to restore the database
   as quickly as possible. You should check DynamoDB write provisioning to make sure it is not throttling writes,
   and make sure you're using a large enough node that you are not CPU bound.

   This function can run for a *very* long time, depending on database size (days or even weeks).

   See `restore-segment!` for a slower version that returns after each segment is restored.

   The arguments are:

   * source-database-name - The name used for this backup.
   * target-conn - The connection of the target db on which to restore.
   * backup-store - A durable store that was used to save the database
   * options - A map of other options
   ** :blacklist - A set of keywords (attributes) to elide from all transactions.
   ** :rewrite - A map from attribute keyword to a `(fn [attr value] new-value)` that can be used to obfuscate the original value to a new one.
   "
  [source-database-name target-conn backup-store {:keys [blacklist rewrite]
                                                  :or   {blacklist #{}}}]
  (let [current-db                 (d/db target-conn)
        last-restored-t            (or
                                     (::last-source-transaction (d/pull current-db [::last-source-transaction] ::last-source-transaction))
                                     0)
        desired-start-t            (if (and last-restored-t (pos? last-restored-t)) (inc last-restored-t) 1)
        all-segment-info           (sort-by :start-t (dcbp/saved-segment-info backup-store source-database-name))
        segments-remaining         (filter (fn [{:keys [start-t end-t]}]
                                             (or
                                               (> start-t desired-start-t)
                                               (<= start-t desired-start-t end-t)))
                                     all-segment-info)
        transaction-source-channel (async/chan 10)]
    (try
      (when (< desired-start-t 2) (ensure-restore-schema! target-conn))
      (async/go-loop [segment-info (first segments-remaining)
                      remainder (rest segments-remaining)]
        (let [{:keys [start-t end-t]} segment-info
              group-result-channel (async/thread
                                     (try
                                       (log/info "Pre-loading group" start-t end-t)
                                       (dcbp/load-transaction-group backup-store source-database-name start-t end-t)
                                       (catch Throwable t
                                         (log/error t "Failed to load transaction group")
                                         {:failed? true})))]
          (async/>! transaction-source-channel group-result-channel))
        (if (seq remainder)
          (recur (first remainder) (rest remainder))
          (async/close! transaction-source-channel)))
      ;; Aggressively load the segments from the backup store in advance of restore to make sure writes are not blocked
      ;; by reading the groups
      (loop [expected-start-t (:start-t (first segments-remaining))]
        (let [{:keys [refs id->attr transactions start-t end-t failed?] :as tgi} (some-> transaction-source-channel
                                                                                   ;; Get the thread's channel
                                                                                   (async/<!!)
                                                                                   ;; Get the thread's result
                                                                                   (async/<!!))
              current-db      (d/db target-conn)
              last-restored-t (p ::last-source-transaction
                                (or
                                  (::last-source-transaction (d/pull current-db [::last-source-transaction] ::last-source-transaction))
                                  0))]
          (cond
            (and start-t (not= start-t expected-start-t))
            #_=> {:error (format "Segments misaligned. Expected to start at %d but found %d" expected-start-t start-t)}
            (and tgi (not failed?))
            #_=> (do
                   (log/info "Restoring transactions from" start-t "to" end-t)
                   (profile {}
                     (doseq [{:keys [t] :as tx-entry} transactions
                             :when (and (> t last-restored-t))]
                       (let [db           (d/db target-conn)
                             target-refs  (all-refs db)
                             resolved-txn (resolved-txn {:db          db
                                                         :id->attr    id->attr
                                                         :source-refs refs} tx-entry)
                             pruned-txn   (prune-tempids-as-values target-refs resolved-txn)
                             final-txn    (rewrite-and-filter-txn {:to-one?   (partial to-one? (d/db target-conn))
                                                                   :id->attr  id->attr
                                                                   :blacklist blacklist
                                                                   :rewrite   rewrite} pruned-txn)]
                         (try
                           (when (empty? final-txn)
                             (throw (ex-info "Incorrect transaction didn't record restore (empty!)" {})))
                           (p ::run-transaction
                             (d/transact target-conn {:tx-data final-txn
                                                      :timeout 10000000}))
                           (catch Throwable e
                             (log/error e "Restore transaction failed!"
                               {:db      source-database-name
                                :message (ex-message e)
                                :data    (ex-data e)
                                :t       t}))))))
                   (recur (inc end-t)))
            :else {:result (str "No more segments to restore. Was looking for the next start: " expected-start-t)})))
      (finally
        (async/close! transaction-source-channel)))))

(defn backup-gaps
  "Looks for gaps in the given segments (maps of :start-t and :end-t inclusive, which is what the db store's
   `saved-segment-info` returns).

   Returns a possibly empty sequece of maps containing the :start-t (inclusive) and :end-t (exclusive) of any gaps that
   are found in the segment list. These are what can be passed to `backup-segment!` to fill that gap."
  [all-segments]
  (let [all-segments (sort-by :start-t all-segments)]
    (persistent!
      (:gaps
        (reduce
          (fn [{:keys [last-end-t] :as acc} {:keys [start-t end-t] :as segment}]
            (cond
              (and last-end-t (< start-t last-end-t)) (do
                                                        (log/warn "Segment" segment "overlaps another segment.")
                                                        acc)
              (and last-end-t (not= start-t (inc last-end-t)))
              (-> acc
                (assoc :last-end-t end-t)
                (update :gaps conj! {:start-t (inc last-end-t)
                                     :end-t   start-t}))
              :ese (assoc acc :last-end-t end-t)))
          {:gaps (transient [])}
          all-segments)))))

(defn repair-backup!
  "Looks for problems with the backup of `dbname` in `db-store` and repairs them. This means it:

   * Finds gaps in the backup segments and runs backups of them.

   WARNING: repairing large gaps can cause a lot of I/O on the database. You may want to check the gaps
   using `backup-gaps` before running this blindly.
   "
  [dbname conn db-store]
  (let [saved-segments (dcbp/saved-segment-info db-store dbname)
        gaps           (backup-gaps (or saved-segments []))]
    (if (empty? gaps)
      (log/info "There were no gaps in the backup")
      (doseq [{:keys [start-t end-t] :as gap} gaps]
        (log/infof "Found gap %s. Backing it up." gap)
        (backup-segment! dbname conn db-store start-t end-t)))))

(comment
  (tufte/add-basic-println-handler! {}))
