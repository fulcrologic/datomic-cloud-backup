(ns com.fulcrologic.datomic-cloud-backup.ram-stores
  (:require
    [com.fulcrologic.datomic-cloud-backup.protocols :as dcbp]))

(deftype RAMDBStore [storage]
  dcbp/BackupStore
  (saved-segment-info [_ dbname]
    (into []
      (map (fn [k]
             (-> (get-in @storage [dbname k])
               (select-keys [:start-t :end-t]))))
      (keys (get @storage dbname))))
  (save-transactions! [_ dbname transaction-group]
    (let [{:keys [start-t]} transaction-group]
      (swap! storage
        assoc-in [dbname start-t] transaction-group)))
  (load-transaction-group [_ dbname start-t]
    (let [real-start (if (= start-t 0)
                       (reduce min 100 (keys (get @storage dbname)))
                       start-t)]
      (get-in @storage [dbname real-start]))))

(defn new-ram-store []
  (->RAMDBStore (atom {})))

(deftype RAMIDMapper [storage]
  dcbp/IDMapper
  (store-id-mappings! [_ dbname source-id->target-id]
    (swap! storage update dbname merge source-id->target-id))
  (resolve-id [_ dbname source-id]
    (get-in @storage [dbname source-id])))

(defn new-ram-mapper []
  (->RAMIDMapper (atom {})))
