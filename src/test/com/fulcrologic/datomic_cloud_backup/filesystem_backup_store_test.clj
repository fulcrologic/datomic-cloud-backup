(ns com.fulcrologic.datomic-cloud-backup.filesystem-backup-store-test
  (:require
    [com.fulcrologic.datomic-cloud-backup.filesystem-backup-store :as fs]
    [com.fulcrologic.datomic-cloud-backup.protocols :as dcbp]
    [fulcro-spec.core :refer [specification behavior component assertions =>]]
    [taoensso.timbre :as log]))

(specification "Filesystem Store"
  (let [store  (fs/new-filesystem-store "/tmp/backup")
        group1 {:info         {:a 1}
                :transactions {:b 2}
                :start-t      1
                :end-t        2}
        group2 {:info         {:c 1}
                :transactions {:d 2}
                :start-t      3
                :end-t        6}
        saved? (try
                 (dcbp/save-transactions! store :db group1)
                 (dcbp/save-transactions! store :db group2)
                 true
                 (catch Exception e
                   (log/error e "Failed to save")
                   false))]

    (assertions
      "Can save transactions"
      saved? => true
      "Can return the sorted list of available segments that can be restored"
      (dcbp/saved-segment-info store :db)
      => [{:start-t 1
           :end-t   2
           :filename "db.1-2.nippy"}
          {:start-t 3
           :end-t   6
           :filename "db.3-6.nippy"}]
      "Can find the last saved segment"
      (dcbp/last-segment-info store :db) => {:start-t 3 :end-t 6}
      "Can retrieve a segment by its start-t"
      (dcbp/load-transaction-group store :db 3) => group2)))