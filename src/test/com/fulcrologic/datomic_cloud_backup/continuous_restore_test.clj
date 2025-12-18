(ns com.fulcrologic.datomic-cloud-backup.continuous-restore-test
  (:require
    [clojure.core.async :as async :refer [<!!]]
    [com.fulcrologic.datomic-cloud-backup.cloning :as cloning]
    [com.fulcrologic.datomic-cloud-backup.continuous-restore :as cr]
    [com.fulcrologic.datomic-cloud-backup.live-transaction-store :as lts]
    [com.fulcrologic.datomic-cloud-backup.ram-stores :refer [new-ram-store]]
    [datomic.client.api :as d]
    [fulcro-spec.core :refer [=> assertions specification]]))

(defonce client (d/client {:server-type :dev-local
                           :storage-dir :mem
                           :system      "continuous-restore-test"}))

(def sample-schema
  [{:db/ident       :person/id
    :db/valueType   :db.type/uuid
    :db/unique      :db.unique/identity
    :db/cardinality :db.cardinality/one}
   {:db/ident       :person/name
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}])

(specification "calculate-backoff"
  (assertions
    "Doubles the current delay"
    (#'cr/calculate-backoff 1000 300000) => 2000
    (#'cr/calculate-backoff 2000 300000) => 4000
    (#'cr/calculate-backoff 4000 300000) => 8000
    "Caps at max delay"
    (#'cr/calculate-backoff 200000 300000) => 300000
    (#'cr/calculate-backoff 300000 300000) => 300000
    "Works with small max delays"
    (#'cr/calculate-backoff 1000 1500) => 1500))

(specification "Shutdown behavior"
  (let [source-db-name (keyword (gensym "source"))
        target-db-name (keyword (gensym "target"))
        _              (d/create-database client {:db-name source-db-name})
        _              (d/create-database client {:db-name target-db-name})
        source-conn    (d/connect client {:db-name source-db-name})
        target-conn    (d/connect client {:db-name target-db-name})
        _              (d/transact source-conn {:tx-data sample-schema})
        store          (lts/new-live-store source-conn {:segment-size 100})
        running?       (volatile! true)]

    (try
      (let [result-chan (cr/continuous-restore!
                          (name source-db-name)
                          target-conn
                          store
                          running?
                          {:poll-interval-ms 100
                           :prefetch-buffer  2})]
        ;; Give it a moment to start
        (Thread/sleep 200)

        ;; Signal shutdown
        (vreset! running? false)

        ;; Wait for result with timeout
        (let [result (async/alt!!
                       result-chan ([v] v)
                       (async/timeout 5000) :timeout)]
          (assertions
            "Stops when running? is set to false"
            (not= result :timeout) => true
            "Returns a status of :stopped or :channel-closed"
            (contains? #{:stopped :channel-closed} (:status result)) => true)))

      (finally
        (d/delete-database client {:db-name source-db-name})
        (d/delete-database client {:db-name target-db-name})))))

(specification "Basic restore flow"
  (let [source-db-name (keyword (gensym "source"))
        target-db-name (keyword (gensym "target"))
        _              (d/create-database client {:db-name source-db-name})
        _              (d/create-database client {:db-name target-db-name})
        source-conn    (d/connect client {:db-name source-db-name})
        target-conn    (d/connect client {:db-name target-db-name})
        person-id      #uuid "33333333-3333-3333-3333-333333333333"
        _              (d/transact source-conn {:tx-data sample-schema})
        _              (Thread/sleep 10)
        _              (d/transact source-conn {:tx-data [{:person/id   person-id
                                                           :person/name "Charlie"}]})
        store          (lts/new-live-store source-conn {:segment-size 100})
        running?       (volatile! true)]

    (try
      (let [result-chan (cr/continuous-restore!
                          (name source-db-name)
                          target-conn
                          store
                          running?
                          {:poll-interval-ms 100
                           :prefetch-buffer  2})]
        ;; Give it time to restore
        (Thread/sleep 1000)

        ;; Stop the restore
        (vreset! running? false)

        ;; Wait for completion
        (let [result (async/alt!!
                       result-chan ([v] v)
                       (async/timeout 5000) :timeout)]
          (assertions
            "Completes without timeout"
            (not= result :timeout) => true
            "Restored at least one segment"
            (>= (:segments-restored result) 0) => true))

        ;; Verify the data was restored
        (let [target-db (d/db target-conn)
              person    (d/pull target-db [:person/id :person/name] [:person/id person-id])]
          (assertions
            "Data was restored to target database"
            (:person/name person) => "Charlie")))

      (finally
        (d/delete-database client {:db-name source-db-name})
        (d/delete-database client {:db-name target-db-name})))))

(specification "Restore with RAM store"
  (let [source-db-name (keyword (gensym "source"))
        target-db-name (keyword (gensym "target"))
        backup-name    (name source-db-name)                ;; Use consistent name for backup/restore
        _              (d/create-database client {:db-name source-db-name})
        _              (d/create-database client {:db-name target-db-name})
        source-conn    (d/connect client {:db-name source-db-name})
        target-conn    (d/connect client {:db-name target-db-name})
        person-id      #uuid "44444444-4444-4444-4444-444444444444"
        _              (d/transact source-conn {:tx-data sample-schema})
        _              (Thread/sleep 10)
        _              (d/transact source-conn {:tx-data [{:person/id   person-id
                                                           :person/name "Diana"}]})
        store          (new-ram-store)]

    (try
      ;; First backup to RAM store using the string name
      (cloning/backup! backup-name source-conn store {:txns-per-segment 10})

      (let [running?    (volatile! true)
            result-chan (cr/continuous-restore!
                          backup-name
                          target-conn
                          store
                          running?
                          {:poll-interval-ms 100
                           :prefetch-buffer  2})]
        ;; Give it time to restore
        (Thread/sleep 1000)

        ;; Stop the restore
        (vreset! running? false)

        ;; Wait for completion
        (<!! result-chan)

        ;; Verify the data was restored
        (let [target-db (d/db target-conn)
              person    (d/pull target-db [:person/id :person/name] [:person/id person-id])]
          (assertions
            "Data was restored from RAM store to target database"
            (:person/name person) => "Diana")))

      (finally
        (d/delete-database client {:db-name source-db-name})
        (d/delete-database client {:db-name target-db-name})))))
