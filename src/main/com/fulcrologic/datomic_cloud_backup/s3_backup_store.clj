(ns com.fulcrologic.datomic-cloud-backup.s3-backup-store
  "AWS implementation of a BackupStore on S3.

   NOTES:

   * Since s3 is eventually consistent it is possible that any reported data from
   methods like `last-segment-info` are incorrect. This should be ok in most cases because backing up
   a segment is an idempotent operation.
   * S3 bills by request.
   ** Choosing a small number of transactions per segment leads to more PUT requests.
   ** The `last-segment-info` is the cheapest way to figure out where to resume, since
   it is a single request. Using `saved-segment-info` lists all objects in the store, and can result
   in a lot of requests and network overhead.
   "
  (:require
    [clojure.string :as str]
    [com.fulcrologic.datomic-cloud-backup.protocols :as dcbp]
    [taoensso.encore :as enc]
    [taoensso.nippy :as nippy]
    [taoensso.timbre :as log])
  (:import
    (java.io File)
    (software.amazon.awssdk.auth.credentials DefaultCredentialsProvider)
    (software.amazon.awssdk.core.sync RequestBody)
    (software.amazon.awssdk.services.s3 S3Client)
    (software.amazon.awssdk.services.s3.model GetObjectRequest
                                              HeadObjectRequest
                                              ListObjectsV2Request PutObjectRequest NoSuchKeyException PutObjectRequest S3Object)))

(defn aws-credentials? []
  (boolean
    (enc/catching
      (let [cp    (DefaultCredentialsProvider/create)
            creds (.resolveCredentials cp)]
        (some? creds)))))

(defn new-s3-client
  "Creates an s3 client. S3 clients are cacheable as long as credentials have not changed. The connection itself is created
  per request.

  DO NOT USE DIRECTLY. Use large object stores (one per bucket) instead, since they can be configured for easy dev override."
  ^S3Client []
  (try
    (-> (S3Client/builder)
      (.credentialsProvider (DefaultCredentialsProvider/create))
      (.build))
    (catch Exception e
      (log/error "Cannot create s3 connection" (.getMessage e)))))

(defn get-object
  "Get an object from s3. Be sure to use this with with-open to ensure the stream is closed when you are done with it.

  ```
  (with-open [stream (get-object ...)]
     ...)
  ```

  Use `get-object-as-string` for a method that can obtain the string content of an object without such cleanup measures.
  "
  ^bytes [^S3Client aws-client ^String bucket-name ^String key]
  (let [^GetObjectRequest req (-> (GetObjectRequest/builder)
                                (.bucket bucket-name)
                                (.key key)
                                (.build))]
    (.asByteArray (.getObjectAsBytes aws-client req))))

(defn put-object
  "Put an object with `key` in the given bucket. The string-or-file can be
   a string or a java.io.File (in the arity-3 version). The arity 4 version requires that the object be some kind
   of InputStream. Returns a PutObjectResponse or throws an exception."
  [^S3Client aws-client ^String bucket-name key ^bytes bs]
  (let [^PutObjectRequest req (-> (PutObjectRequest/builder)
                                (.bucket bucket-name)
                                (.key key)
                                (.build))]
    (.putObject aws-client req (RequestBody/fromBytes bs))))

(defn object-exists? [^S3Client aws-client ^String bucket ^String nm]
  (let [^HeadObjectRequest req (-> (HeadObjectRequest/builder)
                                 (.bucket bucket)
                                 (.key nm)
                                 (.build))]
    (try
      (.headObject aws-client req)
      true
      (catch NoSuchKeyException _ false))))


(defn- put-compressed-edn
  "Compress the given EDN and store it as `key` in the given bucket."
  [^S3Client client ^String bucket-name key EDN]
  (when-not bucket-name
    (throw (ex-info "Cannot write file. No bucket name" {})))
  (try
    (let [^bytes data (nippy/freeze EDN)]
      (put-object client bucket-name key data))
    (catch Throwable t
      (log/error t "Unable to upload EDN" key))))

(defn- get-compressed-edn
  "Get a nippy-compressed object from s3 and decompress it back to EDN."
  [^S3Client client ^String bucket-name ^String resource-name]
  (let [tempFile (File. resource-name)]
    (let [^bytes data (get-object client bucket-name resource-name)
          edn         (nippy/thaw data)]
      edn)))

(defn- list-objects
  "List all of the objects in the given bucket that have the given prefix"
  [^S3Client aws-client ^String bucket ^String prefix]
  (let [^ListObjectsV2Request req (-> (ListObjectsV2Request/builder)
                                    (.bucket bucket)
                                    (.prefix prefix)
                                    (.build))]
    (loop [lor     (.listObjectsV2 aws-client req)
           results []]
      (let [results (into results
                      (map (fn [^S3Object s]
                             (str/replace-first (.key s) prefix "")))
                      (.contents lor))]
        (if (.isTruncated lor)
          (let [^ListObjectsV2Request req (-> (ListObjectsV2Request/builder)
                                            (.bucket bucket)
                                            (.prefix prefix)
                                            (.continuationToken (.nextContinuationToken lor))
                                            (.build))]
            (recur
              (.listObjectsV2 aws-client req)
              results))
          results)))))

(defn- artifact-basename [dbname start-t] (format "/%s/%d/" (name dbname) start-t))
(defn- artifact-name [dbname start-t end-t] (format "/%s/%d/%d/transaction-group.nippy" (name dbname) start-t end-t))
(defn- last-saved-segment-name [dbname] (format "/%s/last-segment.nippy" (name dbname)))

(deftype S3BackupStore [^S3Client aws-client ^String bucket-name]
  dcbp/BackupStore
  (last-segment-info [this dbname]
    (let [object-name (last-saved-segment-name dbname)
          data        (when (object-exists? aws-client bucket-name object-name)
                        (get-compressed-edn aws-client bucket-name object-name))]
      (if data
        data
        (last (dcbp/saved-segment-info this dbname)))))
  (saved-segment-info [_ dbname]
    (let [stored-segments (list-objects aws-client bucket-name (str "/" (name dbname)))
          infos           (keep (fn [nm]
                                  (let [[_ start end] (re-find #"^/(\d+)/(\d+)/.*$" nm)]
                                    (when (and start end)
                                      {:start-t (Long/parseLong start)
                                       :end-t   (Long/parseLong end)})))
                            stored-segments)]
      (vec (sort-by :start-t infos))))
  (save-transactions! [_ dbname transaction-group]
    (let [{:keys [start-t end-t]} transaction-group
          nm (artifact-name dbname start-t end-t)]
      (put-compressed-edn aws-client bucket-name nm transaction-group)
      (put-compressed-edn aws-client bucket-name (last-saved-segment-name dbname) {:start-t start-t :end-t end-t})))
  (load-transaction-group
    [this dbname start-t]
    (let [start-t   (if (= 0 start-t)
                      (:start-t (first (dcbp/saved-segment-info this dbname)))
                      start-t)
          nm        (first (list-objects aws-client bucket-name (artifact-basename dbname start-t)))
          full-name (str (artifact-basename dbname start-t) nm)]
      (get-compressed-edn aws-client bucket-name full-name)))
  (load-transaction-group
    [_ dbname start-t end-t]
    (let [full-name (artifact-name dbname start-t end-t)]
      (get-compressed-edn aws-client bucket-name full-name))))

(defn new-s3-store
  ([bucket s3-options]
   (let [client (new-s3-client)]
     (->S3BackupStore client bucket)))
  ([bucket]
   (new-s3-store bucket {})))

(comment
  (System/getenv)
  (list-objects (new-s3-client) "dataico-staging-backups" "/staging-shard-3/99400"))
