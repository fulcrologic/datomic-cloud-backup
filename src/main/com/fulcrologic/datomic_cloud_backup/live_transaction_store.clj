(ns com.fulcrologic.datomic-cloud-backup.live-transaction-store
  "A TransactionStore implementation that reads directly from a live Datomic database connection.

   This store provides read-only access to transaction history from a source database,
   presenting it through the same TransactionStore interface used by backup stores.
   Useful for direct database-to-database cloning without intermediate storage."
  (:require
    [com.fulcrologic.datomic-cloud-backup.cloning :as cloning]
    [com.fulcrologic.datomic-cloud-backup.protocols :as dcbp]
    [com.fulcrologic.guardrails.core :refer [=> >defn]]
    [datomic.client.api :as d]
    [datomic.client.api.protocols :as dp]
    [taoensso.timbre :as log]))

(defn- ensure-id->attr-cache!
  "Lazily compute and cache the id->attr mapping. Uses db as-of year 2000
   to get the base schema mappings."
  [source-conn id->attr-cache]
  (if-let [cached @id->attr-cache]
    cached
    (let [db       (d/db source-conn)
          as-of-db (d/as-of db #inst "2000-01-01")
          mapping  (cloning/id->attr as-of-db)]
      (log/debug "Computed id->attr cache with" (count mapping) "entries")
      (vreset! id->attr-cache mapping)
      mapping)))

(deftype LiveTransactionStore [source-conn segment-size id->attr-cache]
  dcbp/TransactionStore

  (last-segment-info [_ _dbname]
    (let [db        (d/db source-conn)
          current-t (:t db)]
      (log/debug "last-segment-info: current db t =" current-t)
      {:start-t 1
       :end-t   current-t}))

  (saved-segment-info [_ _dbname]
    (let [db        (d/db source-conn)
          current-t (:t db)
          segments  (loop [start  1
                           result []]
                      (if (> start current-t)
                        result
                        (let [end (min (+ start (dec segment-size)) current-t)]
                          (recur (inc end)
                            (conj result {:start-t start :end-t end})))))]
      (log/debug "saved-segment-info: generated" (count segments) "virtual segments up to t =" current-t)
      segments))

  (save-transactions! [_ _dbname _transaction-group]
    ;; No-op for live store - we read directly from the source
    (log/debug "save-transactions! called on LiveTransactionStore (no-op)")
    nil)

  (load-transaction-group [this dbname start-t]
    (let [end-t (+ start-t (dec segment-size))]
      (log/debug "load-transaction-group (2-arity): start-t =" start-t "computed end-t =" end-t)
      (dcbp/load-transaction-group this dbname start-t end-t)))

  (load-transaction-group [_ _dbname start-t end-t]
    (let [db           (d/db source-conn)
          current-t    (:t db)
          ;; Cap end-t at current database t
          capped-end-t (min end-t current-t)]
      ;; Early return if start-t is beyond current database state
      (if (> start-t current-t)
        (do
          (log/warn "Requested start-t" start-t "is beyond current db t" current-t)
          nil)
        (let [_        (log/info "Loading transactions from source: start-t =" start-t
                         "end-t =" end-t "capped-end-t =" capped-end-t)
              ;; tx-range end is exclusive, so we use (inc capped-end-t)
              tx-range (d/tx-range source-conn {:start start-t
                                                :end   (inc capped-end-t)
                                                :limit -1})
              tx-vec   (vec tx-range)]
          (if (empty? tx-vec)
            (do
              (log/warn "No transactions found in range" start-t "to" capped-end-t)
              nil)
            (let [actual-start (-> tx-vec first :t)
                  actual-end   (-> tx-vec last :t)
                  refs         (cloning/all-refs db)
                  id->attr     (ensure-id->attr-cache! source-conn id->attr-cache)
                  transactions (mapv #(update % :data cloning/mapify-datoms) tx-vec)]
              (log/info "Loaded" (count transactions) "transactions, actual range:"
                actual-start "to" actual-end)
              {:refs         refs
               :id->attr     id->attr
               :transactions transactions
               :start-t      actual-start
               :end-t        actual-end})))))))

(>defn new-live-store
  "Create a LiveTransactionStore that reads directly from a source Datomic connection.

   This store presents the transaction log of a live database through the TransactionStore
   protocol, enabling direct database-to-database cloning without intermediate storage.

   Arguments:
   - source-conn: A Datomic connection to read transactions from

   Options:
   - :segment-size - Number of transactions per virtual segment (default 1000)"
  [source-conn {:keys [segment-size] :or {segment-size 1000}}]
  [#(satisfies? dp/Connection %) map? => ::dcbp/store]
  (log/info "Creating LiveTransactionStore with segment-size =" segment-size)
  (->LiveTransactionStore source-conn segment-size (volatile! nil)))
