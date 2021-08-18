(ns com.fulcrologic.datomic-cloud-backup.cloning
  "Copy a database with history via transaction logs. Based on code from Cognitect."
  (:require
    [clojure.pprint :refer [pprint]]
    [clojure.set :as set]
    [com.fulcrologic.datomic-cloud-backup.protocols :as dcbp]
    [datomic.client.api :as d]
    [taoensso.timbre :as log])
  (:import (clojure.lang ExceptionInfo)))

(def abort-import? (atom false))

(defn all-refs
  "Return a set of all :db.type/ref entity IDs from the provided DB"
  [db]
  (into #{}
    (map first
      (d/q '[:find ?a
             :in $
             :where
             [?e :db/valueType :db.type/ref]
             [?e :db/ident ?a]] db))))

(defn build-ident-lookup
  "Creates a map from Datomic ident to eid for the given database."
  [db]
  (into {}
    (d/q '[:find ?ident ?e
           :in $
           :where [?e :db/ident ?ident]]
      db)))

(defn datom->map [{:keys [e a v tx added]}]
  {:e     e
   :a     a
   :v     v
   :tx    tx
   :added added})

(defn mapify-datoms
  [datoms]
  (mapv datom->map datoms))

(defn backup-segment!
  "Takes an explicit transaction range, and copies it from the database to
   the `backup-store`.  `end-t` is exclusive.

   Returns the real {:start-t a :end-t b} that was actually stored, which may be nil
   if nothing was stored."
  [database-name source-conn backup-store start-t end-t]
  (let [db                 (d/db source-conn)
        ref-attrs          (all-refs db)
        ident-lookup-map   (build-ident-lookup db)
        database-info      {:ident-lookup-map ident-lookup-map
                            :ref-attrs        ref-attrs}
        tx-group           (d/tx-range source-conn {:start start-t :end end-t :limit -1})
        serializable-group (mapv #(update % :data mapify-datoms) tx-group)
        actual-start       (-> (vec tx-group) first :t)
        actual-end         (-> tx-group last :t)]
    (if (and actual-start actual-end)
      (do
        (dcbp/save-transactions! backup-store database-name
          {:info         database-info
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

(defn ^:deprecated parallel-backup!
  "DEPRECATED. Use backup! instead.

  Creates parallel threads to back up a range of the given database using the storage name `dbname` to
   the given `store`. This uses `pmap` for the threading.

  * `store` is the backup store to write to (existing segments will be overwritten if they cover and exact same range).
  * `txns-per-segment` is the max number of transactions you want to put into each backup segment. The final segment
    will probably have fewer.

  Returns a sequence of `{:start-t a :end-t b}` of the segment created (where both numbers are inclusive of actual data
  written).
  "
  [dbname connection store txns-per-segment]
  (backup! dbname connection store {:txns-per-segment txns-per-segment
                                    :parallel?        true
                                    :starting-offset  0}))

(defn normalize-txn [{:keys [id->attr rewrite blacklist]} txn]
  (update txn :data (fn [datoms]
                      (->> datoms
                        (sort-by first)
                        (map
                          (fn [{:keys [e a v added]}]
                            (let [a (id->attr a)
                                  e (if (= a :db/txInstant) "datomic.tx" e)
                                  v (if-let [rewrite (and added (get rewrite a))]
                                      (rewrite a v)
                                      v)]
                              [(if added :db/add :db/retract) e a v])))
                        (remove #(contains? blacklist (nth % 2)))))))

(defn tx-time [{:keys [data]}]
  (last (first (filter #(= :db/txInstant (nth % 2)) data))))

(defn normalize-txns [{:keys [id->attr blacklist rewrite]} txns]
  (into []
    (comp
      (map (partial normalize-txn {:id->attr  id->attr
                                   :rewrite   rewrite
                                   :blacklist blacklist}))
      ;; NOTE: The drop is here to skip the stuff that is auto-created by Datomic itself during create-database.
      ;; It is my best guess, and *seems* to work, but could break with different versions.
      (drop-while
        (fn [txn]
          (let [tm (tx-time txn)]
            (boolean
              (or
                (nil? tm)
                (< (compare tm #inst "1980-01-01") 0)))))))
    txns))

(defn -load-transactions
  "Protocols don't mock well. This is just a wrapper to facilitate a testing scenario."
  [backup-store dbname start-t]
  (dcbp/load-transaction-group backup-store dbname start-t))

(defn restore-segment!
  "Restore a segment of a backup.


  * source-database-name - The name used during backup.
  * conn - The connection of the target db on which to restore.
  * backup-store - A durable store that was used to save the database
  * id-mapper - A durable ID mapper that was used when saving the segments to backup-store
  * start-t - Where to resume restoration from. Should be 0 for the first segment, and then the value returned by the
   prior invocation.
  * options - A map of other options
  ** :blacklist - A set of keywords (attributes) to elide from all transactions.
  ** :rewrite - A map from attribute keyword to a `(fn [attr value] new-value)` that can be used to obfuscate the original value to a new one.

  There is a global atom in this ns called `abort-import?` which this function sets to false. If you set it to true it
  will cause this restore to terminate early.

  WARNING: You should avoid calling this function when you are rebooting/restarting the machine. There is
  a small time window between running a transaction and saving the tempid remappings that, if interrupted,
  will lose the mapping, which in turn can corrupt your recovery. Ideally you would have something like
  a Redis flag that you could toggle in order to pause your restore so you can safely redeploy.

  Returns the next `start-t` that is needed to continue restoration.
  "
  [source-database-name conn backup-store id-mapper start-t {:keys [blacklist rewrite]
                                                             :or   {blacklist #{}}}]
  (let [{:keys [end-t info transactions] :as tgi} (-load-transactions backup-store source-database-name start-t)
        {ref?              :ref-attrs
         attr->original-id :ident-lookup-map} info
        target-attr->id          (build-ident-lookup (d/db conn))
        id->attr                 (set/map-invert attr->original-id)
        incoming-id->target-id   (into {}
                                   (keep (fn [[attr id]]
                                           (when-let [target-id (get target-attr->id attr)]
                                             [id target-id]))) attr->original-id)
        transactions-of-interest (normalize-txns {:id->attr  id->attr
                                                  :blacklist blacklist
                                                  :rewrite   rewrite} transactions)
        resolve                  (fn [id] (or (dcbp/resolve-id id-mapper source-database-name id) (str id)))]

    ;; Make sure any new schema (idents) have proper id resolution
    (dcbp/store-id-mappings! id-mapper source-database-name incoming-id->target-id)

    (when (empty? transactions-of-interest)
      (log/info "Skipping transaction range" (:start-t tgi) "-" end-t
        "It is empty (early transactions are datomic-specific and are never restored)."))

    (doseq [{:keys [t data]} transactions-of-interest]
      (let [data          (mapv
                            (fn [[op e a v]]
                              (let [e (resolve e)
                                    v (if (ref? a)
                                        (resolve v)
                                        v)]
                                [op e a v]))
                            data)
            {:keys [tempids
                    error]} (try
                              (d/transact conn {:tx-data data
                                                :timeout 10000000})
                              (catch ExceptionInfo e
                                (let [{:db/keys [error]} (ex-data e)]
                                  (if (= error :db.error/past-tx-instant)
                                    (do
                                      (log/warn "Transaction was already processed. Skipping" source-database-name t)
                                      {:tempids {}})
                                    {:error e})))
                              (catch Exception e
                                (log/error e "Restore transaction failed!" source-database-name t)
                                {:error e}))
            addl-rewrites (reduce-kv (fn [acc tempid realid]
                                       (if (= tempid "datomic.tx")
                                         acc
                                         (assoc acc (Long/parseLong tempid) realid)))
                            {}
                            tempids)]
        (dcbp/store-id-mappings! id-mapper source-database-name addl-rewrites)
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


