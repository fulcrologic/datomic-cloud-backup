(ns com.fulcrologic.datomic-cloud-backup.cloning-test
  (:require
    [datomic.client.api :as d]
    [com.fulcrologic.datomic-cloud-backup.ram-stores :refer [new-ram-store]]
    [com.fulcrologic.datomic-cloud-backup.protocols :as dcbp]
    [com.fulcrologic.datomic-cloud-backup.cloning :as cloning]
    [com.fulcrologic.datomic-cloud-backup.s3-backup-store :refer [new-s3-store aws-credentials?]]
    [com.fulcrologic.datomic-cloud-backup.filesystem-backup-store :refer [new-filesystem-store]]
    [fulcro-spec.core :refer [specification behavior component assertions => provided]]
    [com.fulcrologic.datomic-cloud-backup.filesystem-backup-store :as fs]
    [clojure.string :as str]
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
      (Thread/sleep 10)
      (recur (cloning/backup-next-segment! dbname source-connection target-store 2)))))

(defn restore! [dbname target-conn db-store]
  (while (= :restored-segment (cloning/restore-segment! dbname target-conn db-store {}))))

(defn clean-filesystem! [^File tmpdir]
  (when (and (.exists tmpdir) (.isDirectory tmpdir) (str/starts-with? (.getAbsolutePath tmpdir) "/t"))
    (doseq [backup-file (filter
                          (fn [^File nm] (str/ends-with? (.getName nm) ".nippy"))
                          (file-seq tmpdir))]
      (.delete backup-file))))

(def sample-schema
  [{:db/ident       :transaction/reason
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident       :person/id
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
    :db/cardinality :db.cardinality/one}])

(defn run-tests [dbname db-store]
  (let [source-db-name (keyword (gensym "db"))
        target-db-name (keyword (gensym "db"))
        person-id      (UUID/randomUUID)
        address-id     (UUID/randomUUID)
        txns           [sample-schema
                        [[:db/add "BAD-DATOM" :person/address 4872362574]
                         [:db/add "datomic.tx" :transaction/reason "Because"]
                         {:db/id          "BOB"
                          :person/id      person-id
                          :person/name    "Bob"
                          :person/address {:db/id          "MAIN"
                                           :address/id     address-id
                                           :address/street "123 Main"}}]]
        _              (d/create-database client {:db-name source-db-name})
        _              (d/create-database client {:db-name target-db-name})
        conn           (d/connect client {:db-name source-db-name})
        target-conn    (d/connect client {:db-name target-db-name})
        {{:strs [BOB MAIN]} :tempids} (last (mapv (fn [txn]
                                                    (Thread/sleep 1)
                                                    (d/transact conn {:tx-data txn})) txns))]
    ;; Hack to make sure IDs don't align internally, so that less likely to get false success
    (d/transact target-conn {:tx-data [{:db/ident       :thing/id
                                        :db/valueType   :db.type/long
                                        :db/unique      :db.unique/identity
                                        :db/cardinality :db.cardinality/one}
                                       {:db/ident       :other/id
                                        :db/valueType   :db.type/long
                                        :db/unique      :db.unique/identity
                                        :db/cardinality :db.cardinality/one}
                                       [:db/add "datomic.tx" :db/txInstant #inst "1970-01-01T12:00"]]})
    (d/transact target-conn {:tx-data [{:db/id    "new-thing"
                                        :thing/id 99}
                                       [:db/add "datomic.tx" :db/txInstant #inst "1970-01-01T12:01"]]})

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
        (restore! dbname target-conn db-store)

        (let [restored-db (d/db target-conn)
              person      (d/pull restored-db [:person/id :person/name
                                               ::cloning/original-id
                                               {:person/address [:address/id :address/street
                                                                 ::cloning/original-id]}]
                            [:person/id person-id])]
          (assertions
            "Can restore the database in pieces"
            (dissoc person :db/id) => {:person/id            person-id
                                       ::cloning/original-id BOB
                                       :person/name          "Bob"
                                       :person/address       {:address/id           address-id
                                                              ::cloning/original-id MAIN
                                                              :address/street       "123 Main"}})))
      (finally
        (d/delete-database client {:db-name source-db-name})
        (d/delete-database client {:db-name target-db-name})))))

(specification "Adding tracking schema"
  (let [db-name (keyword (gensym "db"))
        _       (d/create-database client {:db-name db-name})
        conn    (d/connect client {:db-name db-name})]

    (cloning/ensure-restore-schema! conn)

    (let [db (d/db conn)]
      (assertions
        "Adds the proper starting location"
        (-> (d/datoms db {:index      :eavt
                          :components [::cloning/last-source-transaction ::cloning/last-source-transaction]})
          first
          :v)
        => 0))))

(specification "Resolving IDs"
  (let [db-name    (keyword (gensym "db"))
        _          (d/create-database client {:db-name db-name})
        conn       (d/connect client {:db-name db-name})
        person-id  (UUID/randomUUID)
        person2-id (UUID/randomUUID)
        person3-id (UUID/randomUUID)
        person4-id (UUID/randomUUID)
        _          (cloning/ensure-restore-schema! conn)
        tx!        (fn [c tx] (as-> (d/transact c {:tx-data tx}) $
                                (assoc {}
                                  :data (:tx-data $)
                                  :tempids (:tempids $)
                                  :t (dec (:t (d/db c))))))
        _          (tx! conn sample-schema)
        {{:strs [PERSON1 PERSON2]} :tempids} (tx! conn [{:db/id                "PERSON1"
                                                         ::cloning/original-id 99
                                                         :person/id            person-id
                                                         :person/name          "Joe"}
                                                        {:db/id                "PERSON2"
                                                         ::cloning/original-id 100
                                                         :person/id            person2-id
                                                         :person/name          "Sam"}
                                                        {:db/id                "PERSON3"
                                                         ::cloning/original-id 101
                                                         :person/id            person3-id
                                                         :person/name          "Sally"}
                                                        {:db/id                "PERSON4"
                                                         ::cloning/original-id 101
                                                         :person/id            person4-id
                                                         :person/name          "Dupe Sally"}])]
    (assertions
      "Leaves keywords alone"
      (cloning/resolve-id {:db (d/db conn)} :x) => :x
      "Rewrites transaction ID number to \"datomic.tx\""
      (cloning/resolve-id {:db (d/db conn)
                           :tx-id 44} 44) => "datomic.tx"
      "Can find original entities by ::original-id"
      (cloning/resolve-id {:db (d/db conn)} 99) => PERSON1
      (cloning/resolve-id {:db (d/db conn)} 100) => PERSON2
      "Throws an exception if the original ID isn't unique"
      (cloning/resolve-id {:db (d/db conn)} 101) =throws=> #"Two entities share the same original ID")))

(specification "Bookkeeping transaction"
  (let [db-name        (keyword (gensym "db"))
        target-db-name (keyword (gensym "db"))
        _              (d/create-database client {:db-name db-name})
        _              (d/create-database client {:db-name target-db-name})
        conn           (d/connect client {:db-name db-name})
        target-conn    (d/connect client {:db-name target-db-name})
        person-id      (UUID/randomUUID)
        person2-id     (UUID/randomUUID)
        tx!            (fn [c tx] (as-> (d/transact c {:tx-data tx}) $
                                    (assoc {}
                                      :data (:tx-data $)
                                      :tempids (:tempids $)
                                      :t (dec (:t (d/db c))))))
        schema-entry   (tx! conn sample-schema)
        {:keys [tempids] :as tx1-entry} (tx! conn [{:db/id       "PERSON1"
                                                    :person/id   person-id
                                                    :person/name "Joe"}])
        {:strs [PERSON1]} tempids
        {:keys [tempids] :as tx2-entry} (tx! conn [{:person/id   person-id
                                                    :person/name "Bob"}
                                                   {:db/id       "PERSON2"
                                                    :person/id   person2-id
                                                    :person/name "Mary"}])
        {:strs [PERSON2]} tempids]
    (try
      (cloning/ensure-restore-schema! target-conn)
      (tx! target-conn [{:db/ident       :boogers/mcgee
                         :db/cardinality :db.cardinality/one
                         :db/valueType   :db.type/string}])

      (let [db     (d/db target-conn)
            datoms (cloning/bookkeeping-txn {:db db} schema-entry)
            {:keys [t]} schema-entry]
        (assertions
          "Adds a CAS operation to ensure the entry being restored in the correct one"
          (first datoms) => [:db/cas ::cloning/last-source-transaction ::cloning/last-source-transaction 0 t]
          "Adds original IDs to the schema attributes"
          (every? (fn [[add tmpid k id]]
                    (and
                      (= add :db/add)
                      (string? tmpid)
                      (= k ::cloning/original-id)
                      (int? id))) (rest datoms)) => true)


        (tx! target-conn sample-schema)

        (let [_      (d/transact target-conn
                       {:tx-data
                        [{:db/id                (str PERSON1)
                          ::cloning/original-id PERSON1
                          :person/id            person-id
                          :person/name          "Joe"}]})
              tx-id  (->> tx2-entry :data (filter (fn [{:keys [v]}] (inst? v))) (map :e) (first))
              db     (d/db target-conn)
              datoms (cloning/bookkeeping-txn {:db db} tx2-entry)]
          (assertions
            "Fixes tempids on the tx and new items"
            (vec (rest datoms)) => [[:db/add "datomic.tx" ::cloning/original-id tx-id]
                                    [:db/add (str PERSON2) ::cloning/original-id PERSON2]])))

      (finally
        (d/delete-database client {:db-name db-name})
        (d/delete-database client {:db-name target-db-name})))))

;; Flaky test...it passes if run enough times, and fails for poor reasons. No time to rewrite
(specification "resolved-txn"
  (let [db-name        (keyword (gensym "db"))
        target-db-name (keyword (gensym "db"))
        _              (d/create-database client {:db-name db-name})
        _              (d/create-database client {:db-name target-db-name})
        conn           (d/connect client {:db-name db-name})
        target-conn    (d/connect client {:db-name target-db-name})
        person-id      (UUID/randomUUID)
        person2-id     (UUID/randomUUID)
        tx!            (fn [c tx] (as-> (d/transact c {:tx-data tx}) $
                                    (assoc {}
                                      :data (:tx-data $)
                                      :tempids (:tempids $)
                                      :t (dec (:t (d/db c))))))
        schema-entry   (tx! conn sample-schema)
        {:keys [tempids] :as tx1-entry} (tx! conn [{:db/id       "PERSON1"
                                                    :person/id   person-id
                                                    :person/name "Joe"}])
        {:strs [PERSON1]} tempids
        {:keys [tempids] :as tx2-entry} (tx! conn [{:person/id   person-id
                                                    :person/name "Bob"}
                                                   {:db/id       "PERSON2"
                                                    :person/id   person2-id
                                                    :person/name "Mary"}])
        id->attr       (cloning/id->attr (d/as-of (d/db conn) #inst "2000-01-01"))
        {:strs [PERSON2]} tempids]
    (try
      (cloning/ensure-restore-schema! target-conn)
      (tx! target-conn [{:db/ident       :boogers/mcgee
                         :db/cardinality :db.cardinality/one
                         :db/valueType   :db.type/string}])

      (let [db             (d/db target-conn)
            source-refs    (cloning/all-refs (d/db conn))
            original-tx-id (-> schema-entry :data first :tx)
            txn            (cloning/resolved-txn {:db          db
                                                  :id->attr    id->attr
                                                  :source-refs source-refs} schema-entry)
            txn-set        (set txn)]
        (component "When dealing with early schema"
          (assertions
            "Adds original IDs to user schema attributes"
            (contains? txn-set [:db/add "77" :com.fulcrologic.datomic-cloud-backup.cloning/original-id 77]) => true
            "Rewrites the db id of the txn to datomic.tx"
            (contains? txn-set [:db/add "datomic.tx" :com.fulcrologic.datomic-cloud-backup.cloning/original-id original-tx-id]) => true
            "Rewrites the :db/id of the new items to strings that match the original ids"
            (contains? txn-set [:db/add "74" :db/ident :person/id]) => true
            "Uses the temp ids as the values for install attribute"
            (contains? txn-set [:db/add :db.part/db :db.install/attribute "74"]) => true))

        (tx! target-conn sample-schema)

        (let [{{:strs [NEW-PERSON1]} :tempids} (d/transact target-conn
                                                 {:tx-data
                                                  [{:db/id                "NEW-PERSON1"
                                                    ::cloning/original-id PERSON1
                                                    :person/id            person-id
                                                    :person/name          "Joe"}]})
              db             (d/db target-conn)
              source-refs    (cloning/all-refs (d/db conn))
              txn            (cloning/resolved-txn {:db          db
                                                    :id->attr    id->attr
                                                    :source-refs source-refs} tx2-entry)
              original-tx-id (-> tx2-entry :data first :tx)]
          (assertions
            "Includes the transaction sequence CAS"
            (ffirst txn) => :db/cas
            "Adds the original ID to the transaction"
            (second txn) => [:db/add
                             "datomic.tx"
                             :com.fulcrologic.datomic-cloud-backup.cloning/original-id
                             original-tx-id]
            "Adds original IDs to new entities"
            (nth txn 2) => [:db/add
                            (str PERSON2)
                            :com.fulcrologic.datomic-cloud-backup.cloning/original-id
                            PERSON2]
            "Includes the original transaction time"
            (-> txn (nth 3) butlast) => [:db/add "datomic.tx" :db/txInstant]
            "Uses real IDs for updating things that are in the database"
            ;; NOTE: The strings for attributes are because we are not doing the actual restore,
            ;; so it cannot find the original IDs
            (subvec txn 4 6) => [[:db/add NEW-PERSON1 "75" "Bob"]
                                 [:db/retract NEW-PERSON1 "75" "Joe"]]
            "Uses correct tmpid for new entities"
            (map second (subvec txn 6)) => [(str PERSON2) (str PERSON2)])))

      (finally
        (d/delete-database client {:db-name db-name})
        (d/delete-database client {:db-name target-db-name})))))

(specification "Backup"
  (component "Using Test Stores (RAM-Based)"
    (run-tests :db1 (new-ram-store)))
  (component "Using Filesystem"
    (let [tmpdirfile (.toFile (Files/createTempDirectory "" (make-array FileAttribute 0)))
          tempdir    (.getAbsolutePath tmpdirfile)]
      (run-tests :db1 (new-filesystem-store tempdir))
      (clean-filesystem! tmpdirfile)))
  (component "Using S3"
    (if (and (aws-credentials?))
      (let [dbname (keyword (gensym "test"))]
        (run-tests dbname (new-s3-store "datomic-cloning-test-bucket")))
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

      (let [segments       (cloning/backup! db-name conn fs-store {:parallel?        true
                                                                   :txns-per-segment 100})
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
        (clean-filesystem! tmpdir)
        (d/delete-database client {:db-name db-name})))))

(specification "backup-gaps"
  (assertions
    "Returns a sequence of gaps that are found in the provided database segments"
    (cloning/backup-gaps [{:start-t 100
                           :end-t   105}
                          {:start-t 110
                           :end-t   118}
                          #_{:start-t 109
                             :end-t   130}
                          #_{:start-t 109
                             :end-t   145}
                          {:start-t 146
                           :end-t   163}])
    => [{:start-t 106 :end-t 110}
        {:start-t 119 :end-t 146}]))

(specification "repair-backup!"
  (let [db-name        (keyword (gensym "db"))
        target-db-name (keyword (gensym "db"))
        schema         [{:db/ident       :person/id
                         :db/valueType   :db.type/long
                         :db/unique      :db.unique/identity
                         :db/cardinality :db.cardinality/one}
                        {:db/ident       :person/name
                         :db/valueType   :db.type/string
                         :db/cardinality :db.cardinality/one}]
        tempdirfile    (.toFile (Files/createTempDirectory "" (make-array FileAttribute 0)))
        tempdir        (.getAbsolutePath tempdirfile)
        store          (new-filesystem-store tempdir)
        _              (d/create-database client {:db-name db-name})
        conn           (d/connect client {:db-name db-name})
        _              (d/transact conn {:tx-data schema})]

    (try
      (dotimes [n 1000]
        (d/transact conn {:tx-data [{:person/id   n
                                     :person/name (str "Bob " n)}]}))

      (cloning/backup-segment! db-name conn store 1 300)
      (cloning/backup-segment! db-name conn store 330 565)
      (cloning/backup-segment! db-name conn store 575 1000)
      (cloning/backup-segment! db-name conn store 1000 1100)

      (assertions
        "When there is a gap in the backup"
        (count (cloning/backup-gaps (dcbp/saved-segment-info store db-name))) => 2)

      (cloning/repair-backup! db-name conn store)

      (assertions
        "Creates the missing file(s)"
        (count (cloning/backup-gaps (dcbp/saved-segment-info store db-name))) => 0)

      (let [_           (d/create-database client {:db-name target-db-name})
            target-conn (d/connect client {:db-name target-db-name})]
        (while (= :restored-segment (cloning/restore-segment! db-name target-conn store {}))
          (cloning/restore-segment! db-name target-conn store {}))
        (let [db  (d/db target-conn)
              cnt (ffirst (try (d/q '[:find (count ?p) :where [?p :person/name]] db)
                               (catch Exception e nil)))]
          (assertions
            "The repaired backup contains all of the original entities"
            cnt => 1000)))

      (finally
        (clean-filesystem! tempdirfile)
        (d/delete-database client {:db-name target-db-name})
        (d/delete-database client {:db-name db-name})))))

(specification "Resuming an Interrupted Restore"
  (let [db-name        (keyword (gensym "db"))
        target-db-name (keyword (gensym "db"))
        schema         [{:db/ident       :person/id
                         :db/valueType   :db.type/long
                         :db/unique      :db.unique/identity
                         :db/cardinality :db.cardinality/one}
                        {:db/ident       :person/name
                         :db/valueType   :db.type/string
                         :db/cardinality :db.cardinality/one}]
        store          (new-ram-store)
        _              (d/create-database client {:db-name db-name})
        conn           (d/connect client {:db-name db-name})
        _              (d/transact conn {:tx-data schema})]

    (try
      (dotimes [n 101]
        (Thread/sleep 1)                                    ; important. if too fast then tx time doesn't change and we get a false success on test
        (d/transact conn {:tx-data [{:person/id   n
                                     :person/name (str "Bob " n)}]}))

      (cloning/backup-segment! db-name conn store 1 10)
      (cloning/backup-segment! db-name conn store 10 20)
      (cloning/backup-segment! db-name conn store 20 (:t (d/db conn)))

      (let [_           (d/create-database client {:db-name target-db-name})
            target-conn (d/connect client {:db-name target-db-name})]

        (assertions
          "Restore :restored-segment when it restores some data"
          (cloning/restore-segment! db-name target-conn store {}) => :restored-segment)

        (let [group          (dcbp/load-transaction-group store db-name 10)
              real-load-txns cloning/-load-transactions]
          (provided "The restore restores only PART of a segment"
            ;; The first time we simulate a failure midway through restore
            (cloning/-load-transactions s n start) =1x=> (update group :transactions subvec 0 5)
            ;; The rest of the time we do what we are asked
            (cloning/-load-transactions s n start) => (real-load-txns s n start)

            ;; First attempt on this segment only gets half of them
            (cloning/restore-segment! db-name target-conn store {})
            ;; Retry should get the rest, an complain about the duplicates
            (cloning/restore-segment! db-name target-conn store {})
            ;; Load the remainder of the database
            (cloning/restore-segment! db-name target-conn store {})

            (assertions
              "Resuming succeeds"
              (ffirst
                (d/q '[:find (count ?id)
                       :where
                       [?id :person/id]] (d/db target-conn))) => 100))))

      (finally
        (d/delete-database client {:db-name target-db-name})
        (d/delete-database client {:db-name db-name})))))

(specification "restore!! for fast restores"
  (let [db-name        (keyword (gensym "db"))
        target-db-name (keyword (gensym "db"))
        schema         [{:db/ident       :person/id
                         :db/valueType   :db.type/long
                         :db/unique      :db.unique/identity
                         :db/cardinality :db.cardinality/one}
                        {:db/ident       :person/name
                         :db/valueType   :db.type/string
                         :db/cardinality :db.cardinality/one}]
        store          (new-ram-store)
        _              (d/create-database client {:db-name db-name})
        conn           (d/connect client {:db-name db-name})
        _              (d/transact conn {:tx-data schema})]

    (try
      (dotimes [n 101]
        (Thread/sleep 1)                                    ; important. if too fast then tx time doesn't change and we get a false success on test
        (d/transact conn {:tx-data [{:person/id   n
                                     :person/name (str "Bob " n)}]}))

      (cloning/backup! db-name conn store {:txns-per-segment 10
                                           :parallel?        false})

      (let [_           (d/create-database client {:db-name target-db-name})
            target-conn (d/connect client {:db-name target-db-name})
            result      (cloning/restore!! db-name target-conn store {})
            new-db      (d/db target-conn)]

        (assertions
          "Returns a result to indicate it is done"
          result => {:result "No more segments to restore. Was looking for the next start: 108"}
          "Restores the expected data"
          (d/pull new-db [:person/name] [:person/id 1]) => {:person/name "Bob 1"}
          (d/pull new-db [:person/name] [:person/id 100]) => {:person/name "Bob 100"}))

      (finally
        (d/delete-database client {:db-name target-db-name})
        (d/delete-database client {:db-name db-name})))))