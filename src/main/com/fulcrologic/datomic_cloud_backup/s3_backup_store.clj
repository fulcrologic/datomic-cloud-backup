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
    [clojure.java.io :as io]
    [clojure.string :as str]
    [com.fulcrologic.datomic-cloud-backup.protocols :as dcbp]
    [taoensso.nippy :as nippy]
    [taoensso.timbre :as log])
  (:import
    [com.amazonaws.services.s3 AmazonS3ClientBuilder AmazonS3]
    [com.amazonaws.services.s3.model ObjectMetadata S3ObjectSummary]
    (com.amazonaws ClientConfiguration)
    (java.io ByteArrayOutputStream)))

(defn aws-credentials? []
  (let [k (System/getenv "AWS_ACCESS_KEY_ID")]
    (string? k)))

(defn- ^AmazonS3 new-s3-client
  "Creates an s3 client. S3 clients are cacheable as long as credentials have not changed. The connection itself is created
  per request. Assumes you have credentials set in your environment according to AWS documentation."
  ([] (new-s3-client {}))
  ([{:keys [region]}]
   (let [client-config (doto (ClientConfiguration.)
                         ;; the only kind of errors that are retried are those that aws itself responds with as retryable.
                         ;; This setting is intended to make it so that s3 operations retry until they succeed (as long
                         ;; as they *can* succeed. See docstring and https://docs.aws.amazon.com/general/latest/gr/api-retries.html
                         ;; The default is only 3 retries.
                         (.setMaxErrorRetry 30))
         builder       (doto (AmazonS3ClientBuilder/standard)
                         (.setClientConfiguration client-config))]
     (when region
       (.setRegion builder region))
     (.build builder))))

(defn- metadata-with [{:keys [disposition content-type content-length]}]
  (let [object-meta (new ObjectMetadata)]
    (when disposition (.setContentDisposition object-meta disposition))
    (when content-type (.setContentType object-meta content-type))
    (when content-length (.setContentLength object-meta (long content-length)))
    object-meta))

(defn- put-compressed-edn
  "Compress the given EDN and store it as `key` in the given bucket."
  [^AmazonS3 aws-client ^String bucket-name key EDN]
  (when-not bucket-name
    (throw (ex-info "Cannot write file. No bucket name" {})))
  (let [byte-array (nippy/freeze EDN)
        n          (count byte-array)
        stream     (io/input-stream byte-array)]
    (.putObject aws-client bucket-name (str key) stream (metadata-with {:content-type   "application/octet-stream"
                                                                        :content-length n}))))

(defn- get-object [aws-client bucket key]
  (-> (.getObject aws-client bucket key) .getObjectContent))

(defn- get-stream [aws-client bucket-name key target-output-stream]
  (let [nm                      key
        max-delay-seconds       2
        automatic-fetch-retries 10
        fetch!                  #(try
                                   (let [nm key]
                                     (with-open [in (get-object aws-client bucket-name nm)]
                                       (io/copy in target-output-stream)
                                       (.flush target-output-stream))
                                     true)
                                   (catch Exception _e false))
        loop-delay              (int (* 1000.0 (/ (double (max 1 max-delay-seconds)) (double (max 1 automatic-fetch-retries)))))]
    ;; S3 is eventually consistent. The code would not be asking for this resource if it was expected not to exist,
    ;; so we try a few times in case it hasn't arrived on s3 yet.
    (loop [attempt 1
           result  (fetch!)]
      (cond
        (true? result) true
        (>= attempt automatic-fetch-retries) (do
                                               (log/error "Giving up after" automatic-fetch-retries "attempts to read" nm)
                                               false)
        :else (do
                (Thread/sleep loop-delay)
                (recur (inc attempt) (fetch!)))))))

(defn- get-compressed-edn
  "Get a nippy-compressed object from s3 and decompress it back to EDN."
  [^AmazonS3 aws-client ^String bucket-name ^String resource-name]
  (try
    (let [baos (ByteArrayOutputStream.)
          _    (get-stream aws-client bucket-name resource-name baos)
          edn  (nippy/thaw (.toByteArray baos))]
      edn)
    (catch Exception e
      (log/error e "Thaw failed.")
      nil)))

(defn- list-objects
  "List all of the objects in the given bucket that have the given prefix"
  [^AmazonS3 aws-client ^String bucket ^String prefix]
  (loop [object-listing (.listObjects aws-client bucket prefix)
         results        []]
    (let [results (into results
                    (map (fn [^S3ObjectSummary s]
                           (str/replace-first (.getKey s) prefix "")))
                    (.getObjectSummaries object-listing))]
      (if (.isTruncated object-listing)
        (recur (.listNextBatchOfObjects aws-client object-listing) results)
        results))))

(defn- artifact-basename [dbname start-t] (format "/%s/%d/" (name dbname) start-t))
(defn- artifact-name [dbname start-t end-t] (format "/%s/%d/%d/transaction-group.nippy" (name dbname) start-t end-t))
(defn- last-saved-segment-name [dbname] (format "/%s/last-segment.nippy" (name dbname)))

(deftype S3BackupStore [^AmazonS3 aws-client ^String bucket-name]
  dcbp/BackupStore
  (last-segment-info [this dbname]
    (let [object-name (last-saved-segment-name dbname)
          data        (get-compressed-edn aws-client bucket-name object-name)]
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
   (->S3BackupStore (new-s3-client s3-options) bucket))
  ([bucket]
   (new-s3-store bucket {})))