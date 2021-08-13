(ns com.fulcrologic.datomic-cloud-backup.filesystem-backup-store
  (:require
    [clojure.java.io :as io]
    [com.fulcrologic.datomic-cloud-backup.protocols :as dcbp]
    [taoensso.nippy :as nippy]
    [taoensso.timbre :as log])
  (:import
    (java.io File ByteArrayOutputStream)
    (java.util.regex Pattern)))

(defn- put-compressed-edn
  "Compress the given EDN and store it as a file with name in base-directory."
  [^File base-directory ^String name EDN]
  (let [byte-array (nippy/freeze EDN)]
    (io/copy byte-array (File. base-directory name))))

(defn- get-compressed-edn
  "Get a nippy-compressed object from s3 and decompress it back to EDN."
  [^File base-directory ^String name]
  (let [baos (ByteArrayOutputStream.)
        _    (io/copy (File. base-directory name) baos)
        edn  (nippy/thaw (.toByteArray baos))]
    edn))

(defn- list-objects
  "List all of the objects in the given directory that match pattern"
  [^File base-directory ^Pattern pattern]
  (vec
    (keep (fn [^File f]
            (let [[_ start end] (or
                                  (and
                                    (.isFile f)
                                    (re-find pattern (.getName f)))
                                  [])]
              (when (and start end)
                {:start-t  (Long/parseLong start)
                 :end-t    (Long/parseLong end)
                 :filename (.getName f)})))
      (file-seq base-directory))))

(defn- artifact-name [dbname start-t end-t] (format "%s.%d-%d.nippy" (name dbname) start-t end-t))
(defn backup-file-pattern [dbname]
  (re-pattern (str "^\\Q" (name dbname) "\\E\\.(\\d+)-(\\d+)\\..*$")))

(deftype FilesystemBackupStore [^File base-directory]
  dcbp/BackupStore
  (saved-segment-info [_ dbname]
    (let [stored-segments (list-objects base-directory (backup-file-pattern dbname))]
      (vec (sort-by :start-t stored-segments))))
  (save-transactions! [_ dbname transaction-group]
    (let [{:keys [start-t end-t]} transaction-group
          nm (artifact-name dbname start-t end-t)]
      (put-compressed-edn base-directory nm transaction-group)))
  (load-transaction-group [this dbname start-t]
    (let [segments (dcbp/saved-segment-info this dbname)
          start-t  (if (= 0 start-t)
                     (:start-t (first segments))
                     start-t)
          {:keys [filename]} (first (filter #(= start-t (:start-t %)) segments))]
      (get-compressed-edn base-directory filename))))

(defn new-filesystem-store [^String directory]
  (let [d (File. directory)]
    (when (or
            (not (.isDirectory d))
            (not (.canWrite d)))
      (throw (ex-info "The supplied directory for filesystem storage does not exist or is not writeable." {:dir directory})))
    (->FilesystemBackupStore d)))
