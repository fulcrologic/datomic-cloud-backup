(ns com.fulcrologic.datomic-cloud-backup.logging
  "JSON logging support for CloudWatch integration.
   
   Log messages should be maps with a :msg key for the human-readable message.
   Additional keys become structured data that CloudWatch Logs Insights can query.
   
   Example:
   ```
   (log/info {:msg \"Segment restored\"
              :segment-start 1000
              :segment-end 2000
              :duration-ms 1234})
   ```"
  (:require
    [clojure.data.json :as json]
    [taoensso.timbre :as log]
    [taoensso.timbre.appenders.core :as appenders]))

(defn- exception->map
  "Convert an exception to a map suitable for JSON serialization."
  [^Throwable ex]
  (when ex
    (cond-> {:type    (.getName (class ex))
             :message (ex-message ex)}
      (ex-data ex) (assoc :data (ex-data ex))
      (ex-cause ex) (assoc :cause (exception->map (ex-cause ex))))))

(defn make-json-output-fn
  "Creates a Timbre output-fn that prints JSON to stdout for CloudWatch ingestion.
   
   Options:
   - :service-name - Name to include in log entries (default \"datomic-replication\")
   
   Log entries can be maps with a :msg key. Additional keys become structured fields.
   If the log message is not a map, it becomes the :msg field."
  [{:keys [service-name]
    :or   {service-name "datomic-replication"}}]
  (fn [{:keys [level ?ns-str ?line ?err vargs timestamp_] :as _data}]
    (let [first-arg    (first vargs)
          ;; If first arg is a map with :msg, use it as the base
          base-log-map (if (and (map? first-arg) (contains? first-arg :msg))
                         first-arg
                         {:msg (apply str (interpose " " vargs))})
          log-map      (cond-> (merge base-log-map
                                 {:timestamp (force timestamp_)
                                  :level     level
                                  :ns        ?ns-str
                                  :line      ?line
                                  :service   service-name})
                         ?err (assoc :error (exception->map ?err)))]
      (try
        (json/write-str log-map)
        (catch Throwable _
          ;; Fallback if JSON serialization fails
          (json/write-str {:msg       (str "JSON serialization failed: " (pr-str vargs))
                           :level     level
                           :timestamp (force timestamp_)
                           :service   service-name}))))))

(defn configure-json-logging!
  "Configure Timbre for JSON output to stdout.
   
   Options:
   - :min-level - Minimum log level (default :info)
   - :service-name - Service name in log entries (default \"datomic-replication\")"
  [{:keys [min-level service-name]
    :or   {min-level    :info
           service-name "datomic-replication"}}]
  (log/set-config!
    (-> log/default-config
        (assoc :output-fn (make-json-output-fn {:service-name service-name}))
        (assoc :min-level min-level)
        (assoc :appenders {:println (appenders/println-appender {:stream :auto})}))))
