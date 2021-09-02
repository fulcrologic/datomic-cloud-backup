(ns com.fulcrologic.datomic-cloud-backup.cloning
  "Copy a database with history via transaction logs. Based on code from Cognitect."
  (:require
    [com.fulcrologic.guardrails.core :refer [>defn => ?]]
    [clojure.pprint :refer [pprint]]
    [clojure.set :as set]
    [com.fulcrologic.datomic-cloud-backup.protocols :as dcbp]
    [datomic.client.api :as d]
    [datomic.client.api.protocols :as dp]
    [taoensso.timbre :as log]
    [taoensso.truss :refer [have]]
    [clojure.spec.alpha :as s])
  (:import (clojure.lang ExceptionInfo)
           (java.util Date)))

(s/def ::db #(satisfies? dp/Db %))
(s/def ::datom #(int? (:e %)))
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
  (into #{}
    (map first
      (d/q '[:find ?e
             :in $
             :where
             [?e :db/valueType :db.type/ref]] db))))

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
        segments (inc (int (/ (:t db) txns-per-segment)))]
    (doall
      (Map
        (fn [segment-number]
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
                  (> attempt 10) (do
                                   (log/error "BACKUP FAILED. Too many attempts on segment" segment-number [start end])
                                   (throw (ex-info "BACKUP FAILED." {:segment-number segment-number
                                                                     :start-t        start
                                                                     :end-t          end})))
                  (not @ok?) (do
                               (Thread/sleep 1000)
                               (recur (inc attempt)))
                  :else @result)))))
        (range starting-segment segments)))))

(>defn normalize-txn
  [{:keys [to-one? id->attr rewrite blacklist]} transaction]
  [(s/keys
     :req-un [::to-one? ::id->attr]
     :opt-un [::rewrite ::blacklist])
   (s/coll-of ::txn-op :kind vector?) => (s/coll-of ::txn-op :kind vector?)]
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
    vec))

(>defn tx-time [{:keys [data]}]
  [::txn => (? inst?)]
  (last (first (filter #(= :db/txInstant (nth % 2)) data))))

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
  (let [last-datoms (-> (d/tx-range connection {:start 1 :limit -1})
                      last
                      :data)
        tx-id       (-> last-datoms first :tx)
        time        (:v (first (filter #(and (= tx-id (:e %)) (inst? (:v %))) last-datoms)))
        tma         (Date. (long (+ 1000 (inst-ms time))))
        tmb         (Date. (long (+ 2000 (inst-ms time))))]
    [[{:db/id        "datomic.tx"
       :db/txInstant tma}
      {:db/ident       ::original-id
       :db/unique      :db.unique/identity
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
                        (d/datoms db {:index      :avet
                                      :components [:db/ident ::original-id]
                                      :limit      1})))]
    (when-not has-schema?
      (log/info "Adding schema to track :db/id remappings.")
      (doseq [txn (tempid-tracking-schema-txns connection)]
        (d/transact connection {:tx-data txn})))
    true))

(>defn resolve-id
  "Finds the new database's :db/id for the given :db/id, or returns a stringified version of the ID for use as a new
   tempid."
  [{:keys [db tx-id id->attr]} old-id]
  [(s/keys :ref-un [::db ::tx-id ::id->attr]) int? => ::e]
  (let [old-id (get id->attr old-id old-id)]
    (cond
      (= tx-id old-id) "datomic.tx"
      (keyword? old-id) old-id
      :else (or
              (-> (d/datoms db {:index      :avet
                                :components [::original-id old-id]})
                first
                :e)
              (str old-id)))))

(defn bookkeeping-datoms
  [{:keys [db] :as env} {:keys [t data]}]
  [(s/keys :req-un [::db]) ::txn => (s/coll-of ::txn-op :kind vector?)]
  (let [unique-ids                   (into #{} (map :e) (vec data))
        current-t                    (ffirst (d/q '[:find ?t :where [::last-source-transaction ::last-source-transaction ?t]] db))
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
      unique-ids)))

(>defn resolved-txn
  "Computes the transaction to write to the new database."
  [{:keys [db id->attr source-refs] :as env} {:keys [t data] :as tx-entry}]
  [(s/keys :req-un [::db ::id->attr ::source-refs]) ::txn => (s/coll-of ::txn-op :kind vector?)]
  (let [tx-id (-> data first :tx)
        tm    (:v (first (filter #(and (= tx-id (:e %))
                                    (inst? (:v %))) data)))
        env   (assoc env :tx-id tx-id)]
    (if (< (compare tm #inst "2000-01-01") 0)
      []
      (into (bookkeeping-datoms env tx-entry)
        (map
          (fn [{:keys      [e v added]
                original-a :a}]
            (let [op (if added :db/add :db/retract)
                  e  (resolve-id env e)
                  a  (resolve-id env original-a)
                  v  (cond
                       (and (keyword? a) (= "db" (namespace a)) (int? v)) (resolve-id env v)
                       (= a :db.install/attribute) (str v)
                       (contains? source-refs original-a) (resolve-id env v)
                       :else v)]
              [op e a v])))
        data))))

(defn restore-segment!
  "Restore a segment of a backup.


  * source-database-name - The name used during backup.
  * target-conn - The connection of the target db on which to restore.
  * backup-store - A durable store that was used to save the database
  * start-t - Where to resume restoration from. Should be 0 or 1 for the first segment, and then the value returned by the
   prior invocation.
  * options - A map of other options
  ** :blacklist - A set of keywords (attributes) to elide from all transactions.
  ** :rewrite - A map from attribute keyword to a `(fn [attr value] new-value)` that can be used to obfuscate the original value to a new one.

  Returns the next `start-t` that is needed to continue restoration.
  "
  [source-database-name target-conn backup-store start-t {:keys [blacklist rewrite]
                                                          :or   {blacklist #{}}}]
  (when (< start-t 2) (ensure-restore-schema! target-conn))
  (let [{:keys [end-t refs id->attr transactions] :as tgi} (-load-transactions backup-store source-database-name start-t)
        current-db      (d/db target-conn)
        last-restored-t (log/spy :trace "last-t" (::last-source-transaction (d/pull current-db [::last-source-transaction] ::last-source-transaction)))]
    (doseq [{:keys [t data] :as tx-entry} transactions
            :when (> t last-restored-t)]
      (let [db  (d/db target-conn)
            txn (resolved-txn {:db          db
                               :id->attr    id->attr
                               :source-refs refs} tx-entry)
            txn (normalize-txn {:to-one?   (partial to-one? (d/db target-conn))
                                :id->attr  id->attr
                                :blacklist blacklist
                                :rewrite   rewrite} txn)
            {:keys [error]} (try
                              (when (seq txn)
                                (let [result (d/transact target-conn {:tx-data (log/spy :trace txn)
                                                                      :timeout 10000000})]
                                  (log/spy :trace "REMAPS" (:tempids result))
                                  result))
                              (catch ExceptionInfo e
                                (let [{:db/keys [error]} (ex-data e)]
                                  (if (= error :db.error/past-tx-instant)
                                    (do
                                      (log/warn "Transaction was already processed. Skipping" source-database-name t)
                                      {:tempids {}})
                                    (do
                                      (log/error e "Restore transaction failed!"
                                        {:db         source-database-name
                                         :t          t
                                         :tx-content txn})
                                      {:error e}))))
                              (catch Exception e
                                (log/error e "Restore transaction failed!"
                                  {:db         source-database-name
                                   :t          t
                                   :tx-content txn})
                                {:error e}))]
        (when error
          (throw error))))
    (inc end-t)))

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


