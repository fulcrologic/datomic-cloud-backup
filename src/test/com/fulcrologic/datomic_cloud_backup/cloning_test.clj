(ns com.fulcrologic.datomic-cloud-backup.cloning-test
  (:require
    [datomic.client.api :as d]
    [com.fulcrologic.datomic-cloud-backup.ram-stores :refer [new-ram-store new-ram-mapper]]
    [com.fulcrologic.datomic-cloud-backup.protocols :as dcbp]
    [com.fulcrologic.datomic-cloud-backup.cloning :as cloning]
    [com.fulcrologic.datomic-cloud-backup.s3-backup-store :refer [new-s3-store aws-credentials?]]
    [com.fulcrologic.datomic-cloud-backup.filesystem-backup-store :refer [new-filesystem-store]]
    [com.fulcrologic.datomic-cloud-backup.redis-id-mapper :refer [new-redis-mapper available? clear-mappings!]]
    [fulcro-spec.core :refer [specification behavior component assertions =>]]
    [com.fulcrologic.datomic-cloud-backup.filesystem-backup-store :as fs]
    [clojure.java.shell :as sh]
    [clojure.string :as str]
    [clojure.java.io :as io]
    [taoensso.timbre :as log])
  (:import (java.util UUID)
           (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)
           (java.io File)))

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
        address-id     (UUID/randomUUID)
        txns           [[{:db/ident       :person/id
                          :db/valueType   :db.type/uuid
                          :db/unique      :db.unique/identity
                          :db/cardinality :db.cardinality/one}
                         {:db/ident       :person/name
                          :db/valueType   :db.type/string
                          :db/cardinality :db.cardinality/one}
                         {:db/ident       :address/id
                          :db/valueType   :db.type/uuid
                          :db/cardinality :db.cardinality/one}
                         {:db/ident       :address/street
                          :db/valueType   :db.type/string
                          :db/cardinality :db.cardinality/one}
                         {:db/ident       :person/address
                          :db/valueType   :db.type/ref
                          :db/cardinality :db.cardinality/one}]
                        [{:db/id          "BOB"
                          :person/id      person-id
                          :person/name    "Bob"
                          :person/address {:db/id          "MAIN"
                                           :address/id     address-id
                                           :address/street "123 Main"}}]]
        _              (d/create-database client {:db-name source-db-name})
        _              (d/create-database client {:db-name target-db-name})
        conn           (d/connect client {:db-name source-db-name})
        target-conn    (d/connect client {:db-name target-db-name})
        {{:strs [BOB MAIN]} :tempids} (last (mapv (fn [txn] (d/transact conn {:tx-data txn})) txns))
        ;; This is here to make sure IDs don't align
        _              (d/transact target-conn {:tx-data [{:db/ident       :thing/id
                                                           :db/valueType   :db.type/long
                                                           :db/unique      :db.unique/identity
                                                           :db/cardinality :db.cardinality/one}
                                                          {:db/ident       :other/id
                                                           :db/valueType   :db.type/long
                                                           :db/unique      :db.unique/identity
                                                           :db/cardinality :db.cardinality/one}
                                                          [:db/add "datomic.tx" :db/txInstant #inst "2020-01-01"]]})]

    (try
      (component "incremental backup"
        (backup! dbname conn db-store)

        (assertions
          "Can back up the database in pieces"
          (mapv
            #(select-keys % #{:start-t :end-t})
            (dcbp/saved-segment-info db-store dbname)) => [{:start-t 1 :end-t 2}
                                                           {:start-t 3 :end-t 4}
                                                           {:start-t 5 :end-t 6}
                                                           {:start-t 7 :end-t 7}]))

      (component "incremental restore"
        (restore! dbname target-conn db-store mapper)

        (let [restored-db    (d/db target-conn)
              person         (d/pull restored-db [:db/id :person/id :person/name
                                                  {:person/address [:address/id :address/street]}]
                               [:person/id person-id])
              new-bob-id     (dcbp/resolve-id mapper dbname BOB)
              new-address-id (dcbp/resolve-id mapper dbname MAIN)]
          (assertions
            "Resolves the new IDs"
            (int? new-bob-id) => true
            (int? new-address-id) => true
            (not= BOB new-bob-id) => true
            (not= MAIN new-address-id) => true
            "Can restore the database in pieces"
            (dissoc person :db/id) => {:person/id      person-id
                                       :person/name    "Bob"
                                       :person/address {:address/id     address-id
                                                        :address/street "123 Main"}})))
      (finally
        (d/delete-database client {:db-name source-db-name})
        (d/delete-database client {:db-name target-db-name})))))

(specification "Backup"
  (component "Using Test Stores (RAM-Based)"
    (run-tests :db1 (new-ram-store) (new-ram-mapper)))
  (component "Using Filesystem/RAM mapper"
    (let [tempdir (.getAbsolutePath (.toFile (Files/createTempDirectory "" (make-array FileAttribute 0))))]
      (run-tests :db1 (new-filesystem-store tempdir) (new-ram-mapper))))
  (component "Using AWS/Redis"
    (if (and (aws-credentials?) (available? {}))
      (let [dbname (keyword (gensym "test"))]
        (clear-mappings! {} dbname)
        (run-tests dbname (new-s3-store "datomic-cloning-test-bucket") (new-redis-mapper {})))
      (assertions
        "Resources not available. Test skipped"
        true => true))))

(specification "Parallel backup"
  (let [tmpdir      (.toFile (Files/createTempDirectory "test" (make-array FileAttribute 0)))
        db-name     (keyword (gensym "db"))
        _           (d/create-database client {:db-name db-name})
        conn        (d/connect client {:db-name db-name})
        base-id     (atom 0)
        next-id     (fn [] (UUID/fromString (format "ffffffff-ffff-ffff-ffff-%012d" (swap! base-id inc))))
        make-person (fn [] {:person/id   (next-id)
                            :person/name "Bob"})
        _           (d/transact conn {:tx-data [{:db/ident       :person/id
                                                 :db/valueType   :db.type/uuid
                                                 :db/unique      :db.unique/identity
                                                 :db/cardinality :db.cardinality/one}
                                                {:db/ident       :person/name
                                                 :db/valueType   :db.type/string
                                                 :db/cardinality :db.cardinality/one}]})
        fs-store    (fs/new-filesystem-store (.getAbsolutePath tmpdir))]

    (try
      (dotimes [n 1061]
        (d/transact conn {:tx-data [(make-person)]}))

      (let [segments       (cloning/parallel-backup! conn db-name fs-store 100)
            last-stored-t  (-> segments last :end-t)
            {final-data :data
             final-t    :t} (last (d/tx-range conn {:start 1061 :limit -1}))
            final-saved-tx (last (:transactions (dcbp/load-transaction-group fs-store db-name 1000)))]
        (assertions
          "Backs up the correct number of segments"
          (count segments) => 11
          (:t final-saved-tx) => final-t
          "Stops at the last actual transaction in the database"
          last-stored-t => final-t))

      (finally
        (doseq [backup-file (filter
                              (fn [^File nm] (str/ends-with? (.getName nm) ".nippy"))
                              (file-seq tmpdir))]
          (.delete backup-file))
        (.delete tmpdir)
        (d/delete-database client {:db-name db-name})))))





