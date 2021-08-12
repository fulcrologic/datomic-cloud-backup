(ns com.fulcrologic.datomic-cloud-backup.cloning-test
  (:require
    [datomic.client.api :as d]
    [com.fulcrologic.datomic-cloud-backup.ram-stores :refer [new-ram-store new-ram-mapper]]
    [com.fulcrologic.datomic-cloud-backup.protocols :as dcbp]
    [com.fulcrologic.datomic-cloud-backup.cloning :as cloning]
    [com.fulcrologic.datomic-cloud-backup.s3-backup-store :refer [new-s3-store aws-credentials?]]
    [com.fulcrologic.datomic-cloud-backup.redis-id-mapper :refer [new-redis-mapper available? clear-mappings!]]
    [fulcro-spec.core :refer [specification behavior component assertions =>]])
  (:import (java.util UUID)))

(defonce client (d/client {:server-type :dev-local
                           :storage-dir :mem
                           :system      "test"}))

(defn backup! [dbname source-connection target-store]
  (loop [n (cloning/backup-next-segment! dbname source-connection target-store 2)]
    (when (pos? n)
      (recur (cloning/backup-next-segment! dbname source-connection target-store 2)))))

(defn restore! [dbname target-conn db-store mapper]
  (loop [start-t 0]
    (let [next-start (cloning/restore-segment! dbname target-conn db-store mapper start-t {})
          last-t     7]
      (when (<= next-start last-t)
        (recur next-start)))))

(defn run-tests [dbname db-store mapper]
  (let [source-db-name (keyword (gensym "db"))
        target-db-name (keyword (gensym "db"))
        person-id      (UUID/randomUUID)
        txns           [[{:db/ident       :person/id
                          :db/valueType   :db.type/uuid
                          :db/unique      :db.unique/identity
                          :db/cardinality :db.cardinality/one}
                         {:db/ident       :person/name
                          :db/valueType   :db.type/string
                          :db/cardinality :db.cardinality/one}]
                        [{:person/id   person-id
                          :person/name "Bob"}]]
        _              (d/create-database client {:db-name source-db-name})
        _              (d/create-database client {:db-name target-db-name})
        conn           (d/connect client {:db-name source-db-name})
        target-conn    (d/connect client {:db-name target-db-name})
        _              (doseq [txn txns]
                         (d/transact conn {:tx-data txn}))]

    (component "incremental backup"
      (backup! dbname conn db-store)

      (assertions
        "Can back up the database in pieces"
        (dcbp/saved-segment-info db-store dbname) => [{:start-t 1 :end-t 2}
                                                      {:start-t 3 :end-t 4}
                                                      {:start-t 5 :end-t 6}
                                                      {:start-t 7 :end-t 7}]))

    (component "incremental restore"
      (restore! dbname target-conn db-store mapper)

      (let [restored-db (d/db target-conn)
            person      (d/pull restored-db [:person/id :person/name] [:person/id person-id])]
        (assertions
          "Can restore the database in pieces"
          person => {:person/id   person-id
                     :person/name "Bob"})))))

(specification "Backup"
  (component "Using Test Stores (RAM-Based)"
      (run-tests :db1 (new-ram-store) (new-ram-mapper)))
  (component "Using AWS/Redis"
    (if (and (aws-credentials?) (available? {}))
      (let [dbname (keyword (gensym "test"))]
        (clear-mappings! {} dbname)
        (run-tests dbname (new-s3-store "datomic-cloning-test-bucket") (new-redis-mapper {})))
      (assertions
        "Resources not available. Test skipped"
        true => true))))