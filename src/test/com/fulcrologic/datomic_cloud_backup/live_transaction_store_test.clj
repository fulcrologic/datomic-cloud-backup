(ns com.fulcrologic.datomic-cloud-backup.live-transaction-store-test
  (:require
    [datomic.client.api :as d]
    [com.fulcrologic.datomic-cloud-backup.live-transaction-store :as lts]
    [com.fulcrologic.datomic-cloud-backup.protocols :as dcbp]
    [fulcro-spec.core :refer [specification behavior component assertions =>]]))

(defonce client (d/client {:server-type :dev-local
                           :storage-dir :mem
                           :system      "live-store-test"}))

(def sample-schema
  [{:db/ident       :person/id
    :db/valueType   :db.type/uuid
    :db/unique      :db.unique/identity
    :db/cardinality :db.cardinality/one}
   {:db/ident       :person/name
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}])

(specification "LiveTransactionStore"
  (let [db-name (keyword (gensym "live-test-db"))
        _       (d/create-database client {:db-name db-name})
        conn    (d/connect client {:db-name db-name})
        _       (d/transact conn {:tx-data sample-schema})
        _       (d/transact conn {:tx-data [{:person/id   #uuid "11111111-1111-1111-1111-111111111111"
                                             :person/name "Alice"}]})
        _       (d/transact conn {:tx-data [{:person/id   #uuid "22222222-2222-2222-2222-222222222222"
                                             :person/name "Bob"}]})
        store   (lts/new-live-store conn {:segment-size 100})]

    (try
      (component "last-segment-info"
        (let [db        (d/db conn)
              current-t (:t db)
              info      (dcbp/last-segment-info store db-name)]
          (assertions
            "Returns a map with :start-t and :end-t"
            (map? info) => true
            (contains? info :start-t) => true
            (contains? info :end-t) => true
            "Has :start-t of 1"
            (:start-t info) => 1
            "Has :end-t matching the current database t"
            (:end-t info) => current-t)))

      (component "saved-segment-info"
        (let [db        (d/db conn)
              current-t (:t db)
              segments  (dcbp/saved-segment-info store db-name)]
          (assertions
            "Returns a vector of segment info"
            (vector? segments) => true
            "First segment starts at 1"
            (-> segments first :start-t) => 1
            "All segments have :start-t and :end-t"
            (every? #(and (contains? % :start-t) (contains? % :end-t)) segments) => true
            "Last segment ends at current-t or before"
            (<= (-> segments last :end-t) current-t) => true)))

      (component "load-transaction-group"
        (let [group (dcbp/load-transaction-group store db-name 1)]
          (assertions
            "Returns a map with required keys"
            (map? group) => true
            (contains? group :refs) => true
            (contains? group :id->attr) => true
            (contains? group :transactions) => true
            (contains? group :start-t) => true
            (contains? group :end-t) => true
            ":refs is a set"
            (set? (:refs group)) => true
            ":id->attr is a map"
            (map? (:id->attr group)) => true
            ":transactions is a vector"
            (vector? (:transactions group)) => true
            "Has the expected start-t"
            (:start-t group) => 1)))

      (component "save-transactions!"
        (assertions
          "Returns nil (no-op)"
          (dcbp/save-transactions! store db-name {:start-t 1 :end-t 2 :transactions []}) => nil))

      (component "id->attr caching"
        (let [;; Create a new store to test caching
              new-store    (lts/new-live-store conn {:segment-size 100})
              ;; Load twice to trigger cache hit
              first-group  (dcbp/load-transaction-group new-store db-name 1)
              second-group (dcbp/load-transaction-group new-store db-name 1)]
          (assertions
            "Both loads return the same id->attr mapping"
            (:id->attr first-group) => (:id->attr second-group))))

      (finally
        (d/delete-database client {:db-name db-name})))))

(specification "LiveTransactionStore with no transactions beyond start"
  (let [db-name (keyword (gensym "empty-db"))
        _       (d/create-database client {:db-name db-name})
        conn    (d/connect client {:db-name db-name})
        db      (d/db conn)
        store   (lts/new-live-store conn {:segment-size 100})]

    (try
      (let [current-t (:t db)]
        (component "load-transaction-group beyond current-t"
          (assertions
            "Returns nil when start-t is beyond database state"
            (dcbp/load-transaction-group store db-name (inc current-t)) => nil)))

      (finally
        (d/delete-database client {:db-name db-name})))))
