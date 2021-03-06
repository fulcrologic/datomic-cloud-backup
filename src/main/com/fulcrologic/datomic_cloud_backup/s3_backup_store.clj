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
    (com.amazonaws ClientConfiguration)
    (com.amazonaws.services.s3 AmazonS3ClientBuilder AmazonS3)
    (com.amazonaws.services.s3.model ObjectMetadata S3ObjectSummary)
    (com.amazonaws.services.s3.transfer TransferManager TransferManagerBuilder)
    (java.io InputStream ByteArrayOutputStream OutputStream ByteArrayInputStream File FileOutputStream BufferedOutputStream)
    (java.util Date)))

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

(defn- aws-object-exists? [aws-client bucket nm] (.doesObjectExist aws-client bucket nm))

(defn- metadata-with [{:keys [disposition content-type content-length]}]
  (let [object-meta (new ObjectMetadata)]
    (when disposition (.setContentDisposition object-meta disposition))
    (when content-type (.setContentType object-meta content-type))
    (when content-length (.setContentLength object-meta (long content-length)))
    object-meta))

(defn- put-compressed-edn
  "Compress the given EDN and store it as `key` in the given bucket."
  [^TransferManager transfer ^String bucket-name key EDN]
  (when-not bucket-name
    (throw (ex-info "Cannot write file. No bucket name" {})))
  (let [tempFile (File/createTempFile "txnupload" "nippy")]
    (try
      (let [_      (nippy/freeze-to-file tempFile EDN)
            upload (.upload transfer bucket-name (str key) tempFile)]
        (.waitForCompletion upload))
      (catch Throwable t
        (log/error t "Unable to upload EDN" key))
      (finally
        (.delete tempFile)))))

(defn- get-compressed-edn
  "Get a nippy-compressed object from s3 and decompress it back to EDN."
  [^TransferManager transfer ^String bucket-name ^String resource-name]
  (let [tempFile (File/createTempFile "txns" "nippy")]
    (try
      (let [download (.download transfer bucket-name resource-name tempFile)
            _        (.waitForCompletion download)
            edn      (nippy/thaw-from-file tempFile)]
        edn)
      (catch Exception e
        (log/error e "Thaw failed.")
        nil)
      (finally
        (.delete tempFile)))))

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

(deftype S3BackupStore [^AmazonS3 aws-client ^TransferManager transfer ^String bucket-name]
  dcbp/BackupStore
  (last-segment-info [this dbname]
    (let [object-name (last-saved-segment-name dbname)
          data        (when (aws-object-exists? aws-client bucket-name object-name)
                        (get-compressed-edn transfer bucket-name object-name))]
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
      (put-compressed-edn transfer bucket-name nm transaction-group)
      (put-compressed-edn transfer bucket-name (last-saved-segment-name dbname) {:start-t start-t :end-t end-t})))
  (load-transaction-group
    [this dbname start-t]
    (let [start-t   (if (= 0 start-t)
                      (:start-t (first (dcbp/saved-segment-info this dbname)))
                      start-t)
          nm        (first (list-objects aws-client bucket-name (artifact-basename dbname start-t)))
          full-name (str (artifact-basename dbname start-t) nm)]
      (get-compressed-edn transfer bucket-name full-name)))
  (load-transaction-group
    [_ dbname start-t end-t]
    (let [full-name (artifact-name dbname start-t end-t)]
      (get-compressed-edn transfer bucket-name full-name))))

(defn new-s3-store
  ([bucket s3-options]
   (let [client   (new-s3-client s3-options)
         transfer (-> (TransferManagerBuilder/standard)
                    (.withS3Client client)
                    (.build))]
     (->S3BackupStore client transfer bucket)))
  ([bucket]
   (new-s3-store bucket {})))