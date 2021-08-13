(ns com.fulcrologic.datomic-cloud-backup.cloning
  "Copy a database with history via transaction logs. Based on code from Cognitect."
  (:require
    [clojure.pprint :refer [pprint]]
    [clojure.set :as set]
    [com.fulcrologic.datomic-cloud-backup.protocols :as dcbp]
    [datomic.client.api :as d]
    [taoensso.timbre :as log]))

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

(defn backup-next-segment!
  "
  Computes the next transaction range that is missing from `backup-store` and writes up to
  `max-txns` to the store.

  Returns the number of transactions written (if that is `max-txns`, then
  there may be more transaction that need backing up)."
  [database-name source-conn backup-store max-txns]
  (let [db                 (d/db source-conn)
        max-t              (:t db)
        ref-attrs          (all-refs db)
        ident-lookup-map   (build-ident-lookup db)
        database-info      {:ident-lookup-map ident-lookup-map
                            :ref-attrs        ref-attrs}
        {:keys [end-t]
         :or   {end-t 0}} (last (dcbp/saved-segment-info backup-store database-name))
        start-t            (inc end-t)
        last-t             (+ start-t max-txns)
        tx-group           (if (<= start-t max-t)
                             (d/tx-range source-conn {:start start-t :end last-t :limit -1})
                             [])
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
        (inc (- actual-end actual-start)))
      0)))

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

  Returns the next `start-t` that is needed to continue restoration.
  "
  [source-database-name conn backup-store id-mapper start-t {:keys [blacklist rewrite]
                                                             :or   {blacklist #{}}}]
  (let [{:keys [end-t info transactions] :as tgi} (dcbp/load-transaction-group backup-store source-database-name start-t)
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
                              (log/infof "Restoring %s transaction t = %d." source-database-name t)
                              (d/transact conn {:tx-data data
                                                :timeout 10000000})
                              (catch Exception e
                                (log/error e "Restore transaction failed!")
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