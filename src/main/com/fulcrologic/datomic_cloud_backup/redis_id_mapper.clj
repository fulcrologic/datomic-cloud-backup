(ns com.fulcrologic.datomic-cloud-backup.redis-id-mapper
  (:require
    [com.fulcrologic.datomic-cloud-backup.protocols :as dcbp]
    [taoensso.carmine :as car]))

(defn redis-key [dbname]
  (format "datomic-cloud-backup-%s.idmappings" dbname))

(defn available?
  "Verify that the redis store is available. Returns a boolean."
  [redis-connection-options]
  (try
    (let [pong (car/wcar redis-connection-options (car/ping))]
      (= "PONG" pong))
    (catch Exception _ false)))

(deftype RedisIDMapper [redis-connection-options]
  dcbp/IDMapper
  (store-id-mappings! [this dbname source-id->target-id]
    (let [top-key (redis-key dbname)]
      (car/wcar redis-connection-options
        (doall
          (map (fn [[k v]]
                 (car/hset top-key (car/freeze k) (car/freeze v)))
            source-id->target-id)))))
  (resolve-id [this dbname source-id]
    (car/wcar redis-connection-options
      (car/hget (redis-key dbname) (car/freeze source-id)))))

(defn clear-mappings!
  "Forgets prior mappings for dbname. DANGEROUS. If you do this you CANNOT resume incremental restores, but
   it is necessary IF you're starting over."
  [redis-connection-options dbname]
  (car/wcar redis-connection-options
    (car/del (redis-key dbname))))

(defn new-redis-mapper
  "Create an IDMapper that uses Redis.

   The connection options is a map with things like:

   ```
   {:spec {:host \"localhost\"
           :port 6379}}
   ```

   See `taoensso.carmine` docs for more information on redis-connection-options."
  [redis-connection-options]
  (->RedisIDMapper redis-connection-options))