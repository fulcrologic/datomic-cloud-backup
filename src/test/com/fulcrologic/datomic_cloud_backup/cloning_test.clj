(ns com.fulcrologic.datomic-cloud-backup.cloning-test
  (:require
    [clojure.set :as set]
    [clojure.string :as str]
    [com.fulcrologic.datomic-cloud-backup.cloning :as cloning]
    [com.fulcrologic.datomic-cloud-backup.filesystem-backup-store :refer [new-filesystem-store]]
    [com.fulcrologic.datomic-cloud-backup.filesystem-backup-store :as fs]
    [com.fulcrologic.datomic-cloud-backup.protocols :as dcbp]
    [com.fulcrologic.datomic-cloud-backup.ram-stores :refer [new-ram-store]]
    [com.fulcrologic.datomic-cloud-backup.s3-backup-store :refer [aws-credentials? new-s3-store]]
    [datomic.client.api :as d]
    [fulcro-spec.core :refer [=> assertions component provided specification]])
  (:import (java.io File)
           (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)
           (java.util UUID)))

(defonce client (d/client {:server-type :dev-local
                           :storage-dir :mem
                           :system      "test"}))

(defn backup! [dbname source-connection target-store]
  (loop [n (cloning/backup-next-segment! dbname source-connection target-store 2)]
    (when (pos? n)
      (Thread/sleep 10)
      (recur (cloning/backup-next-segment! dbname source-connection target-store 2)))))

(defn restore! [dbname target-conn db-store]
  ;; Reset the ID cache for this database before restoring
  (cloning/reset-id-cache! dbname)
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
      (cloning/resolve-id {:db    (d/db conn)
                           :tx-id 44} 44) => "datomic.tx"
      "Can find original entities by ::original-id"
      (cloning/resolve-id {:db (d/db conn)} 99) => PERSON1
      (cloning/resolve-id {:db (d/db conn)} 100) => PERSON2
      "Throws an exception if the original ID isn't unique"
      (cloning/resolve-id {:db (d/db conn)} 101) =throws=> #"Two entities share the same original ID")))

(specification "Resolving IDs with cache state"
  (let [db-name    (keyword (gensym "db"))
        _          (d/create-database client {:db-name db-name})
        conn       (d/connect client {:db-name db-name})
        person-id  (UUID/randomUUID)
        person2-id (UUID/randomUUID)
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
                                                         :person/name          "Sam"}])]

    ;; Reset the cache to test fresh behavior
    (cloning/reset-id-cache! db-name)
    (let [cache-state (cloning/get-id-cache db-name)]
      (component "With an empty cache (max-eidx = 0)"
        (assertions
          "IDs with eidx > 0 are detected as new (since max-eidx starts at 0)"
          (cloning/is-new-id? cache-state 99) => true

          "resolve-id returns tempid string for IDs detected as new"
          (cloning/resolve-id {:db (d/db conn) :cache-state cache-state} 99) => "99"))

      ;; Simulate having already restored entity 99 -> PERSON1
      (cloning/cache-store! cache-state 99 PERSON1)

      (component "After recording ID 99"
        (assertions
          "lookup returns the mapped ID"
          (cloning/cache-lookup cache-state 99) => PERSON1

          "max-eidx is now 99"
          @(:max-eidx cache-state) => 99

          "ID 100 is still new (eidx > 99)"
          (cloning/is-new-id? cache-state 100) => true

          "ID 50 is NOT new (eidx <= 99)"
          (cloning/is-new-id? cache-state 50) => false

          "resolve-id uses cache for known IDs"
          (cloning/resolve-id {:db (d/db conn) :cache-state cache-state} 99) => PERSON1

          "resolve-id returns tempid string for definitely new IDs"
          (cloning/resolve-id {:db (d/db conn) :cache-state cache-state} 200) => "200")))

    (d/delete-database client {:db-name db-name})))

(specification "Verification of new ID assertion"
  (let [db-name   (keyword (gensym "db"))
        _         (d/create-database client {:db-name db-name})
        conn      (d/connect client {:db-name db-name})
        person-id (UUID/randomUUID)
        _         (cloning/ensure-restore-schema! conn)
        tx!       (fn [c tx] (as-> (d/transact c {:tx-data tx}) $
                               (assoc {}
                                 :data (:tx-data $)
                                 :tempids (:tempids $)
                                 :t (dec (:t (d/db c))))))
        _         (tx! conn sample-schema)
        {{:strs [PERSON1]} :tempids} (tx! conn [{:db/id                "PERSON1"
                                                 ::cloning/original-id 99
                                                 :person/id            person-id
                                                 :person/name          "Joe"}])]
    (try
      (component "verify-new-id-assertion!"
        (assertions
          "Returns true when ID truly doesn't exist"
          (cloning/verify-new-id-assertion! (d/db conn) 9999) => true

          "Throws when ID exists but was thought to be new"
          (cloning/verify-new-id-assertion! (d/db conn) 99) =throws=> #"ID cache assertion failed"))
      (finally
        (d/delete-database client {:db-name db-name})))))

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
            (set (rest datoms)) => #{[:db/add "datomic.tx" ::cloning/original-id tx-id]
                                     [:db/add (str PERSON2) ::cloning/original-id PERSON2]})))

      (finally
        (d/delete-database client {:db-name db-name})
        (d/delete-database client {:db-name target-db-name})))))

;; Flaky test...it passes if run enough times, and fails for poor reasons. No time to rewrite
(specification "resolved-txn" :focus
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
            (contains? (set txn)
              [:db/add
               "datomic.tx"
               :com.fulcrologic.datomic-cloud-backup.cloning/original-id
               original-tx-id]) => true
            "Adds original IDs to new entities"
            (contains? (set txn) [:db/add
                                  (str PERSON2)
                                  :com.fulcrologic.datomic-cloud-backup.cloning/original-id
                                  PERSON2]) => true
            "Includes the original transaction time"
            (butlast
              (first
                (filter (fn [[_ _ a]]
                          (= :db/txInstant a)) txn))) => [:db/add "datomic.tx" :db/txInstant]
            "Uses real IDs for updating things that are in the database"
            ;; NOTE: The strings for attributes are because we are not doing the actual restore,
            ;; so it cannot find the original IDs
            (set/intersection (set txn)
              #{[:db/add NEW-PERSON1 "75" "Bob"]
                [:db/retract NEW-PERSON1 "75" "Joe"]}) => #{[:db/add NEW-PERSON1 "75" "Bob"]
                                                            [:db/retract NEW-PERSON1 "75" "Joe"]}
            "Uses correct tmpid for new entities"
            (contains? (into #{} (map second) txn) (str PERSON2)) => true)))

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

(specification "Filters ::original-id datoms from source DB that was itself a restore target"
  ;; This test reproduces the production bug where stagingStorage was itself created via restore
  ;; and contains ::original-id datoms in its transaction log. When we try to restore from
  ;; stagingStorage to a new target, those ::original-id datoms would cause conflicts because:
  ;; 1. The source has [entity-X ::original-id old-value]
  ;; 2. bookkeeping-txn tries to add [entity-X ::original-id entity-X-source-id]
  ;; 3. Both resolve to the same target entity -> conflict (cardinality-one)
  ;;
  ;; Fix: resolved-txn now filters out ::original-id and ::last-source-transaction datoms
  (let [target-db-name (keyword (gensym "target-db"))
        dbname         "test-filter-original-id"
        _              (d/create-database client {:db-name target-db-name})
        target-conn    (d/connect client {:db-name target-db-name})
        ;; Simulated source entity IDs
        source-entity-1 74766790688843
        source-tx-id    13194139533333
        ;; Create a fake segment where the source DB has ::original-id datoms
        ;; (because it was itself restored from another database)
        fake-segment    {:refs         #{73}  ;; 73 is ::original-id which is a ref-like long
                         :id->attr     {10 :db/ident
                                        40 :db/valueType
                                        41 :db/cardinality
                                        50 :db/txInstant
                                        ;; These are the restore bookkeeping attributes from the SOURCE db
                                        73 :com.fulcrologic.datomic-cloud-backup.cloning/original-id
                                        74 :com.fulcrologic.datomic-cloud-backup.cloning/last-source-transaction}
                         :start-t      13
                         :end-t        13
                         :transactions [{:t    13
                                         :data [;; Transaction timestamp
                                                {:e source-tx-id :a 50 :v #inst "2024-01-01" :tx source-tx-id :added true}
                                                ;; This is the problematic datom - source entity has ::original-id
                                                ;; because source DB was itself a restore target
                                                {:e source-entity-1 :a 73 :v 101155069755470 :tx source-tx-id :added true}
                                                ;; Normal data - entity with a :db/ident
                                                {:e source-entity-1 :a 10 :v :tax/sordicom-minoristas :tx source-tx-id :added true}]}]}
        fake-store      (reify dcbp/TransactionStore
                          (last-segment-info [_ _] {:start-t 13 :end-t 13})
                          (saved-segment-info [_ _] [{:start-t 13 :end-t 13}])
                          (load-transaction-group [_ _ _] fake-segment)
                          (load-transaction-group [_ _ _ _] fake-segment)
                          (save-transactions! [_ _ _] nil))]
    (try
      (cloning/reset-id-cache! dbname)

      (let [result (cloning/restore-segment! dbname target-conn fake-store {})]
        (assertions
          "Should successfully restore by filtering out source's ::original-id datoms"
          result => :restored-segment)

        ;; Verify the entity was created with OUR ::original-id (not the source's)
        (let [db (d/db target-conn)
              restored-entities (d/q '[:find ?e ?orig-id
                                       :where [?e :com.fulcrologic.datomic-cloud-backup.cloning/original-id ?orig-id]]
                                     db)]
          (assertions
            "The restored entity should have ::original-id pointing to the SOURCE entity ID, not the old backup's ID"
            ;; We should have an ::original-id = source-entity-1, NOT 101155069755470
            (some #(= source-entity-1 (second %)) restored-entities) => true)))

      (finally
        (d/delete-database client {:db-name target-db-name})))))

(specification "Filters composite tuple attribute values from source data"
  ;; Composite tuples are auto-generated by Datomic from their component attributes.
  ;; If we try to restore the tuple values directly, Datomic will complain or create
  ;; duplicates. The fix: filter out any datoms for composite tuple attributes.
  ;;
  ;; This test calls resolved-txn directly to verify the filtering.
  (let [target-db-name (keyword (gensym "target-db"))
        _              (d/create-database client {:db-name target-db-name})
        target-conn    (d/connect client {:db-name target-db-name})
        _              (cloning/ensure-restore-schema! target-conn)
        db             (d/db target-conn)
        ;; Source schema mapping - includes :db/tupleAttrs which marks composite tuples
        id->attr       {10  :db/ident
                        40  :db/valueType
                        41  :db/cardinality
                        50  :db/txInstant
                        68  :db/tupleAttrs  ; This marks attributes as composite tuples
                        75  :person/id+name ; The composite tuple attribute
                        76  :person/id
                        77  :person/name}
        source-refs    #{}  ; No refs for this simple test
        source-tx-id   13194139533333
        source-entity  74766790688843
        tx-entry       {:t    13
                        :data [{:e source-tx-id :a 50 :v #inst "2024-01-01" :tx source-tx-id :added true}
                               ;; Entity data with component values
                               {:e source-entity :a 76 :v 42 :tx source-tx-id :added true}
                               {:e source-entity :a 77 :v "Alice" :tx source-tx-id :added true}
                               ;; Schema: entity 75 has :db/tupleAttrs, making it a composite tuple
                               {:e 75 :a 68 :v [76 77] :tx source-tx-id :added true}
                               ;; THIS IS THE PROBLEMATIC DATOM - auto-generated tuple value
                               ;; Should be filtered because attr 75 is a composite tuple
                               {:e source-entity :a 75 :v [42 "Alice"] :tx source-tx-id :added true}]}]
    (try
      (let [result (cloning/resolved-txn {:db          db
                                          :id->attr    id->attr
                                          :source-refs source-refs}
                     tx-entry)
            ;; Extract just the attribute keywords from the result
            result-attrs (into #{} (keep #(when (= :db/add (first %)) (nth % 2))) result)]
        (assertions
          "Should NOT include composite tuple attribute :person/id+name in resolved transaction"
          (contains? result-attrs :person/id+name) => false

          "Should include component attributes in resolved transaction"
          (contains? result-attrs :person/id) => true
          (contains? result-attrs :person/name) => true))
      (finally
        (d/delete-database client {:db-name target-db-name})))))

(specification "Backup of a backup (chained restore)"
  ;; This tests the full scenario: DB1 -> backup -> DB2 -> backup -> DB3
  ;; DB2 will contain ::original-id datoms from the first restore.
  ;; When we backup DB2 and restore to DB3, those datoms must be filtered.
  (let [db1-name       (keyword (gensym "source-db"))
        db2-name       (keyword (gensym "intermediate-db"))
        db3-name       (keyword (gensym "final-db"))
        store1         (new-ram-store)
        store2         (new-ram-store)
        _              (d/create-database client {:db-name db1-name})
        _              (d/create-database client {:db-name db2-name})
        _              (d/create-database client {:db-name db3-name})
        db1-conn       (d/connect client {:db-name db1-name})
        db2-conn       (d/connect client {:db-name db2-name})
        db3-conn       (d/connect client {:db-name db3-name})
        person-id      (random-uuid)]
    (try
      ;; Step 1: Create data in DB1
      (d/transact db1-conn {:tx-data [{:db/ident       :person/id
                                       :db/valueType   :db.type/uuid
                                       :db/unique      :db.unique/identity
                                       :db/cardinality :db.cardinality/one}
                                      {:db/ident       :person/name
                                       :db/valueType   :db.type/string
                                       :db/cardinality :db.cardinality/one}]})
      (d/transact db1-conn {:tx-data [{:person/id   person-id
                                       :person/name "Alice"}]})

      ;; Step 2: Backup DB1 -> store1
      (backup! (name db1-name) db1-conn store1)

      ;; Step 3: Restore store1 -> DB2
      ;; This will add ::original-id datoms to DB2
      (restore! (name db1-name) db2-conn store1)

      ;; Verify DB2 has the data AND the ::original-id bookkeeping
      (let [db2 (d/db db2-conn)]
        (assertions
          "DB2 should have the person"
          (set (d/q '[:find ?name :where [?e :person/name ?name]] db2)) => #{["Alice"]}

          "DB2 should have ::original-id datoms (from restore)"
          (> (count (d/q '[:find ?e ?orig
                           :where [?e :com.fulcrologic.datomic-cloud-backup.cloning/original-id ?orig]]
                      db2))
            0) => true))

      ;; Step 4: Backup DB2 -> store2
      ;; This backup will include the ::original-id datoms
      (backup! (name db2-name) db2-conn store2)

      ;; Step 5: Restore store2 -> DB3
      ;; This should filter out the source's ::original-id datoms and create new ones
      (restore! (name db2-name) db3-conn store2)

      ;; Step 6: Verify DB3 has the data
      (let [db3 (d/db db3-conn)]
        (assertions
          "DB3 should have the person data"
          (set (d/q '[:find ?name :where [?e :person/name ?name]] db3)) => #{["Alice"]}

          "DB3 should have its own ::original-id datoms (not the ones from DB1->DB2)"
          (> (count (d/q '[:find ?e ?orig
                           :where [?e :com.fulcrologic.datomic-cloud-backup.cloning/original-id ?orig]]
                      db3))
            0) => true))

      (finally
        (d/delete-database client {:db-name db1-name})
        (d/delete-database client {:db-name db2-name})
        (d/delete-database client {:db-name db3-name})))))

;; =============================================================================
;; Unit tests for rewrite-and-filter-txn
;; =============================================================================

;; =============================================================================
;; Unit tests for tx-time
;; =============================================================================

(specification "tx-time"
  ;; tx-time extracts the transaction timestamp from a tx-entry's data
  ;; It finds the datom where e = tx-id and v is an inst

  (component "with valid transaction data"
    (let [tx-id 13194139533312
          tx-instant #inst "2025-01-15T12:00:00"
          tx-entry {:t    100
                    :data [{:e tx-id :a 50 :v tx-instant :tx tx-id :added true}
                           {:e 12345 :a 10 :v :some/ident :tx tx-id :added true}
                           {:e 12345 :a 40 :v 23 :tx tx-id :added true}]}]
      (assertions
        "extracts the transaction instant from the tx datom"
        (cloning/tx-time tx-entry) => tx-instant)))

  (component "with transaction instant not first in data"
    (let [tx-id 13194139533312
          tx-instant #inst "2025-06-20T08:30:00"
          ;; tx instant datom is last
          tx-entry {:t    200
                    :data [{:e 99999 :a 10 :v :user/name :tx tx-id :added true}
                           {:e 99999 :a 40 :v "Alice" :tx tx-id :added true}
                           {:e tx-id :a 50 :v tx-instant :tx tx-id :added true}]}]
      (assertions
        "finds the instant even when not first"
        (cloning/tx-time tx-entry) => tx-instant)))

  (component "with no transaction instant (early datomic txns)"
    (let [tx-id 13194139533312
          ;; Some early transactions may not have an explicit txInstant
          tx-entry {:t    1
                    :data [{:e tx-id :a 10 :v :db/ident :tx tx-id :added true}]}]
      (assertions
        "returns epoch fallback when no instant found"
        (cloning/tx-time tx-entry) => #inst "1970-01-01")))

  (component "with multiple datoms on tx entity"
    (let [tx-id 13194139533312
          tx-instant #inst "2025-03-10T14:45:00"
          ;; Transaction entity has multiple datoms (e.g., txInstant AND a custom attr)
          tx-entry {:t    300
                    :data [{:e tx-id :a 50 :v tx-instant :tx tx-id :added true}
                           {:e tx-id :a 99 :v "some-reason" :tx tx-id :added true}
                           {:e 55555 :a 10 :v :foo/bar :tx tx-id :added true}]}]
      (assertions
        "extracts the instant, not other tx entity datoms"
        (cloning/tx-time tx-entry) => tx-instant)))

  (component "with empty data"
    (assertions
      "returns epoch fallback for empty data"
      (cloning/tx-time {:t 1 :data []}) => #inst "1970-01-01")))

;; =============================================================================
;; Unit tests for optimization functions
;; =============================================================================

(specification "eid->eidx"
  ;; The function extracts the 42-bit entity index from a Datomic entity ID
  ;; using bit masking with 0x3FFFFFFFFFF
  ;;
  ;; Datomic Cloud entity IDs have this structure:
  ;;   Bits 62-42: Partition number (14 bits)
  ;;   Bits 41-0:  Entity index (42 bits) - GLOBAL MONOTONIC COUNTER

  (component "bit extraction"
    (assertions
      "extracts the lower 42 bits from an entity ID"
      ;; Simple case: ID that fits entirely in 42 bits
      (cloning/eid->eidx 12345) => 12345

      "returns 0 for entity ID 0"
      (cloning/eid->eidx 0) => 0

      "extracts entity index when partition bits are present"
      ;; An ID with partition number 1 (bit 42 set): 2^42 + 100 = 4398046511204
      ;; Entity index should be 100
      (cloning/eid->eidx 4398046511204) => 100

      "masks off partition bits correctly"
      ;; Max 42-bit value: 0x3FFFFFFFFFF = 4398046511103
      (cloning/eid->eidx 4398046511103) => 4398046511103

      "handles large entity IDs with multiple partition bits set"
      ;; ID with partition = 3 (bits 42 and 43 set): 3 * 2^42 + 999 = 13194139534311
      (cloning/eid->eidx 13194139534311) => 999))

  (component "boundary conditions"
    (assertions
      "handles minimum valid entity ID (1)"
      (cloning/eid->eidx 1) => 1

      "handles entity index at 42-bit boundary"
      ;; 2^42 - 1 = 4398046511103 (max 42-bit value)
      (cloning/eid->eidx 4398046511103) => 4398046511103

      "wraps correctly at 42-bit overflow"
      ;; 2^42 = 4398046511104 should become 0 (only partition bit set)
      (cloning/eid->eidx 4398046511104) => 0

      "handles typical Datomic transaction ID range"
      ;; Transaction IDs often start around 13194139533312 (partition 3, index 0)
      ;; 3 * 2^42 = 13194139533312
      (cloning/eid->eidx 13194139533312) => 0
      ;; 3 * 2^42 + 1000 = 13194139534312
      (cloning/eid->eidx 13194139534312) => 1000))

  (component "eidx-mask constant"
    (assertions
      "mask value is correct (2^42 - 1)"
      cloning/eidx-mask => 0x3FFFFFFFFFF
      cloning/eidx-mask => 4398046511103)))

(specification "build-cardinality-cache"
  ;; This function queries the database to build a cache mapping
  ;; attribute idents to their cardinality idents
  (let [db-name (keyword (gensym "card-cache-test"))
        _       (d/create-database client {:db-name db-name})
        conn    (d/connect client {:db-name db-name})]
    (try
      ;; Add schema with both cardinality types
      (d/transact conn {:tx-data [{:db/ident       :test/to-one-attr
                                   :db/valueType   :db.type/string
                                   :db/cardinality :db.cardinality/one}
                                  {:db/ident       :test/to-many-attr
                                   :db/valueType   :db.type/string
                                   :db/cardinality :db.cardinality/many}
                                  {:db/ident       :test/ref-to-one
                                   :db/valueType   :db.type/ref
                                   :db/cardinality :db.cardinality/one}
                                  {:db/ident       :test/ref-to-many
                                   :db/valueType   :db.type/ref
                                   :db/cardinality :db.cardinality/many}]})

      (let [db    (d/db conn)
            cache (#'cloning/build-cardinality-cache db)]

        (component "returns a map"
          (assertions
            "result is a map"
            (map? cache) => true))

        (component "cardinality-one attributes"
          (assertions
            "maps :test/to-one-attr to :db.cardinality/one"
            (get cache :test/to-one-attr) => :db.cardinality/one

            "maps :test/ref-to-one to :db.cardinality/one"
            (get cache :test/ref-to-one) => :db.cardinality/one))

        (component "cardinality-many attributes"
          (assertions
            "maps :test/to-many-attr to :db.cardinality/many"
            (get cache :test/to-many-attr) => :db.cardinality/many

            "maps :test/ref-to-many to :db.cardinality/many"
            (get cache :test/ref-to-many) => :db.cardinality/many))

        (component "built-in Datomic attributes"
          (assertions
            "includes :db/ident as cardinality one"
            (get cache :db/ident) => :db.cardinality/one

            "includes :db/cardinality as cardinality one"
            (get cache :db/cardinality) => :db.cardinality/one)))

      (finally
        (d/delete-database client {:db-name db-name})))))

(specification "cached-to-one?"
  ;; This function returns a predicate that checks if an attribute
  ;; has cardinality one using a pre-built cache

  (component "with populated cache"
    (let [cache {:user/name     :db.cardinality/one
                 :user/tags     :db.cardinality/many
                 :order/id      :db.cardinality/one
                 :order/items   :db.cardinality/many}
          to-one? (#'cloning/cached-to-one? cache)]

      (assertions
        "returns true for cardinality-one attributes"
        (to-one? :user/name) => true
        (to-one? :order/id) => true

        "returns false for cardinality-many attributes"
        (to-one? :user/tags) => false
        (to-one? :order/items) => false

        "returns false for unknown attributes (not in cache)"
        (to-one? :unknown/attr) => false
        (to-one? :random/thing) => false)))

  (component "with empty cache"
    (let [cache   {}
          to-one? (#'cloning/cached-to-one? cache)]

      (assertions
        "returns false for any attribute (nothing is known)"
        (to-one? :any/attr) => false
        (to-one? :db/ident) => false)))

  (component "returns a function"
    (let [cache   {:foo/bar :db.cardinality/one}
          to-one? (#'cloning/cached-to-one? cache)]

      (assertions
        "result is a function"
        (fn? to-one?) => true

        "can be used as a predicate"
        (ifn? to-one?) => true))))

(specification "record-new-ids!"
  ;; This function records ID mappings from a transaction result's tempids map
  ;; into the cache-state, updating max-eidx as appropriate.
  ;;
  ;; IMPORTANT: The tempid string keys (e.g. "123") are parsed as the OLD entity IDs.
  ;; The old-id determines the eidx used for max-eidx tracking.
  ;; The values are the NEW entity IDs in the target database.

  (component "with valid tempids"
    ;; Reset cache before each test
    (cloning/reset-id-cache! "test-record-ids")
    (let [cache-state (cloning/get-id-cache "test-record-ids")
          ;; Tempids map from d/transact has string keys (old IDs as strings)
          ;; and new DB IDs as values
          ;; old-id 123 -> new-id 74766790688843
          ;; old-id 456 -> new-id 74766790688844
          ;; old-id 789 -> new-id 74766790688845
          tempids     {"123" 74766790688843
                       "456" 74766790688844
                       "789" 74766790688845}]

      (cloning/record-new-ids! cache-state tempids)

      (assertions
        "stores all mappings in cache (old-id -> new-id)"
        (cloning/cache-lookup cache-state 123) => 74766790688843
        (cloning/cache-lookup cache-state 456) => 74766790688844
        (cloning/cache-lookup cache-state 789) => 74766790688845

        "updates max-eidx based on OLD entity IDs (the string keys)"
        ;; max-eidx tracks the highest old-id eidx seen
        ;; eidx of 789 = 789 (since 789 < 2^42)
        @(:max-eidx cache-state) => 789)))

  (component "with non-numeric string keys"
    (cloning/reset-id-cache! "test-record-non-numeric")
    (let [cache-state (cloning/get-id-cache "test-record-non-numeric")
          ;; datomic.tx becomes a key, but it's not numeric
          tempids     {"datomic.tx" 13194139533313
                       "100"        74766790688900}]

      (cloning/record-new-ids! cache-state tempids)

      (assertions
        "ignores non-numeric keys like 'datomic.tx'"
        ;; The function only stores mappings for numeric string keys
        ;; We cannot look up 13194139533313 because it was never stored
        ;; (The datomic.tx key was parsed as nil and skipped)
        (cloning/cache-lookup cache-state 13194139533313) => nil

        "stores numeric string keys correctly"
        (cloning/cache-lookup cache-state 100) => 74766790688900)))

  (component "with empty tempids"
    (cloning/reset-id-cache! "test-record-empty")
    (let [cache-state (cloning/get-id-cache "test-record-empty")
          initial-max @(:max-eidx cache-state)]

      (cloning/record-new-ids! cache-state {})

      (assertions
        "does not modify max-eidx for empty tempids"
        @(:max-eidx cache-state) => initial-max)))

  (component "updates max-eidx monotonically"
    (cloning/reset-id-cache! "test-record-monotonic")
    (let [cache-state (cloning/get-id-cache "test-record-monotonic")]

      ;; First batch: old-id "100" has eidx=100, mapped to some new-id
      (cloning/record-new-ids! cache-state {"100" 999999})

      (assertions
        "max-eidx is 100 after first batch"
        @(:max-eidx cache-state) => 100)

      ;; Second batch: old-id "500" has eidx=500
      (cloning/record-new-ids! cache-state {"500" 999998})

      (assertions
        "max-eidx increases to 500 after second batch"
        @(:max-eidx cache-state) => 500)

      ;; Third batch: old-id "200" has eidx=200 (less than current max)
      (cloning/record-new-ids! cache-state {"200" 999997})

      (assertions
        "max-eidx stays at 500 (doesn't decrease)"
        @(:max-eidx cache-state) => 500))))

;; =============================================================================
;; Unit tests for rewrite-and-filter-txn
;; =============================================================================

(specification "rewrite-and-filter-txn"
  (let [;; Helper: always cardinality one
        to-one-always (constantly true)
        ;; Helper: always cardinality many
        to-one-never (constantly false)
        ;; Minimal required config
        base-env {:to-one? to-one-always
                  :id->attr {}}]

    (component "returns a vector"
      (assertions
        "empty input returns empty vector"
        (cloning/rewrite-and-filter-txn base-env []) => []

        "result is always a vector"
        (vector? (cloning/rewrite-and-filter-txn base-env [[:db/add "e1" :foo/bar "value"]])) => true))

    (component "sorting behavior"
      (assertions
        "adds come before retracts in output"
        (cloning/rewrite-and-filter-txn base-env
          [[:db/retract "e1" :foo/a "old"]
           [:db/add "e1" :foo/b "new"]])
        => [[:db/add "e1" :foo/b "new"]
            [:db/retract "e1" :foo/a "old"]]

        "multiple adds and retracts are properly ordered"
        (cloning/rewrite-and-filter-txn base-env
          [[:db/retract "e1" :foo/a "old1"]
           [:db/retract "e1" :foo/b "old2"]
           [:db/add "e1" :foo/c "new1"]
           [:db/add "e1" :foo/d "new2"]])
        => [[:db/add "e1" :foo/c "new1"]
            [:db/add "e1" :foo/d "new2"]
            [:db/retract "e1" :foo/a "old1"]
            [:db/retract "e1" :foo/b "old2"]]))

    (component "blacklist filtering"
      (assertions
        "removes ops where attribute is in blacklist"
        (cloning/rewrite-and-filter-txn
          (assoc base-env :blacklist #{:secret/password})
          [[:db/add "e1" :user/name "Alice"]
           [:db/add "e1" :secret/password "hunter2"]])
        => [[:db/add "e1" :user/name "Alice"]]

        "removes retracts for blacklisted attributes"
        (cloning/rewrite-and-filter-txn
          (assoc base-env :blacklist #{:secret/password})
          [[:db/add "e1" :user/name "Alice"]
           [:db/retract "e1" :secret/password "oldpass"]])
        => [[:db/add "e1" :user/name "Alice"]]

        "empty blacklist filters nothing"
        (cloning/rewrite-and-filter-txn
          (assoc base-env :blacklist #{})
          [[:db/add "e1" :user/name "Alice"]])
        => [[:db/add "e1" :user/name "Alice"]]

        "nil blacklist filters nothing"
        (cloning/rewrite-and-filter-txn
          (assoc base-env :blacklist nil)
          [[:db/add "e1" :user/name "Alice"]])
        => [[:db/add "e1" :user/name "Alice"]]

        "multiple blacklisted attributes are all filtered"
        (cloning/rewrite-and-filter-txn
          (assoc base-env :blacklist #{:secret/a :secret/b})
          [[:db/add "e1" :user/name "Alice"]
           [:db/add "e1" :secret/a "val1"]
           [:db/add "e1" :secret/b "val2"]])
        => [[:db/add "e1" :user/name "Alice"]]))

    (component "value rewriting"
      (let [obfuscate (fn [_attr val] (str "REDACTED-" (count val)))]
        (assertions
          "rewrites values for matching attributes on adds"
          (cloning/rewrite-and-filter-txn
            (assoc base-env :rewrite {:user/email obfuscate})
            [[:db/add "e1" :user/email "secret@example.com"]])
          => [[:db/add "e1" :user/email "REDACTED-18"]]

          "does NOT rewrite values on retracts"
          (cloning/rewrite-and-filter-txn
            (assoc base-env :rewrite {:user/email obfuscate})
            [[:db/retract "e1" :user/email "secret@example.com"]])
          => [[:db/retract "e1" :user/email "secret@example.com"]]

          "leaves non-matching attributes unchanged"
          (cloning/rewrite-and-filter-txn
            (assoc base-env :rewrite {:user/email obfuscate})
            [[:db/add "e1" :user/name "Alice"]])
          => [[:db/add "e1" :user/name "Alice"]]

          "nil rewrite map leaves all values unchanged"
          (cloning/rewrite-and-filter-txn
            (assoc base-env :rewrite nil)
            [[:db/add "e1" :user/email "test@example.com"]])
          => [[:db/add "e1" :user/email "test@example.com"]]

          "empty rewrite map leaves all values unchanged"
          (cloning/rewrite-and-filter-txn
            (assoc base-env :rewrite {})
            [[:db/add "e1" :user/email "test@example.com"]])
          => [[:db/add "e1" :user/email "test@example.com"]])))

    (component "noop detection (cardinality-one conflict prevention)"
      ;; When rewrite causes add/retract values to match, the retract becomes a noop
      ;; for cardinality-one attributes (would cause datom conflict otherwise)
      (assertions
        "drops retract that follows add on same cardinality-one attribute"
        (cloning/rewrite-and-filter-txn
          {:to-one? to-one-always
           :id->attr {}
           :rewrite {:user/name (fn [_ _] "SAME")}}
          [[:db/add "e1" :user/name "new-value"]
           [:db/retract "e1" :user/name "old-value"]])
        => [[:db/add "e1" :user/name "SAME"]]

        "keeps retract on cardinality-many attribute even with same add"
        (cloning/rewrite-and-filter-txn
          {:to-one? to-one-never
           :id->attr {}
           :rewrite {:user/tags (fn [_ _] "SAME")}}
          [[:db/add "e1" :user/tags "new-value"]
           [:db/retract "e1" :user/tags "old-value"]])
        => [[:db/add "e1" :user/tags "SAME"]
            [:db/retract "e1" :user/tags "old-value"]]

        "keeps retract on different attribute from add"
        (cloning/rewrite-and-filter-txn
          base-env
          [[:db/add "e1" :user/name "Alice"]
           [:db/retract "e1" :user/email "old@example.com"]])
        => [[:db/add "e1" :user/name "Alice"]
            [:db/retract "e1" :user/email "old@example.com"]]

        "noop detection uses to-one? predicate per attribute"
        (cloning/rewrite-and-filter-txn
          {:to-one? (fn [attr] (= attr :user/name))  ; only :user/name is to-one
           :id->attr {}
           :rewrite {:user/name (fn [_ _] "SAME")
                     :user/tags (fn [_ _] "SAME")}}
          [[:db/add "e1" :user/name "new"]
           [:db/retract "e1" :user/name "old"]
           [:db/add "e1" :user/tags "new"]
           [:db/retract "e1" :user/tags "old"]])
        => [[:db/add "e1" :user/name "SAME"]
            [:db/add "e1" :user/tags "SAME"]
            [:db/retract "e1" :user/tags "old"]]))

    (component "combined behaviors"
      (assertions
        "blacklist, rewrite, and sorting work together"
        (cloning/rewrite-and-filter-txn
          {:to-one? to-one-never  ; Use to-one-never so retracts aren't dropped as noops
           :id->attr {}
           :blacklist #{:secret/data}
           :rewrite {:user/email (fn [_ v] (str "REDACTED-" (count v)))}}
          [[:db/retract "e1" :user/name "OldName"]
           [:db/add "e1" :user/email "secret@example.com"]
           [:db/add "e1" :secret/data "should-be-removed"]
           [:db/add "e1" :user/name "NewName"]])
        => [[:db/add "e1" :user/email "REDACTED-18"]
            [:db/add "e1" :user/name "NewName"]
            [:db/retract "e1" :user/name "OldName"]]

        "handles CAS operations (passed through unchanged)"
        (cloning/rewrite-and-filter-txn
          base-env
          [[:db/cas "e1" :user/version 1 2]
           [:db/add "e1" :user/name "Alice"]])
        => [[:db/add "e1" :user/name "Alice"]
            [:db/cas "e1" :user/version 1 2]]))))
