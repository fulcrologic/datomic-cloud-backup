(ns com.fulcrologic.datomic-cloud-backup.redis-id-mapper-test
  (:require
    [com.fulcrologic.datomic-cloud-backup.redis-id-mapper :as rm]
    [com.fulcrologic.datomic-cloud-backup.protocols :as dcbp]
    [fulcro-spec.core :refer [specification behavior component assertions =>]]))

(specification "Redis ID Mapper"
  (if (rm/available? {})
    (let [mapper (rm/new-redis-mapper {})]
      (dcbp/store-id-mappings! mapper :db {Long/MAX_VALUE Long/MIN_VALUE})
      (dcbp/store-id-mappings! mapper :db {0 -1
                                           3 4})

      (assertions
        "Stores cumulative mappings for later resolution"
        (dcbp/resolve-id mapper :db 0) => -1
        (dcbp/resolve-id mapper :db 3) => 4
        (dcbp/resolve-id mapper :db Long/MAX_VALUE) => Long/MIN_VALUE
        "Store actual Long values"
        (type (dcbp/resolve-id mapper :db 0)) => Long))
    (assertions
      "Redis not available. Test skipped"
      true => true)))
