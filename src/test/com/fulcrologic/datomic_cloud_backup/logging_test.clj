(ns com.fulcrologic.datomic-cloud-backup.logging-test
  (:require
    [clojure.data.json :as json]
    [com.fulcrologic.datomic-cloud-backup.logging :as logging]
    [fulcro-spec.core :refer [specification behavior component assertions =>]]))

(specification "make-json-output-fn"
  (let [output-fn (logging/make-json-output-fn {:service-name "test-service"})]

    (component "with map log entry containing :msg"
      (let [log-data   {:level      :info
                        :?ns-str    "test.ns"
                        :?line      42
                        :?err       nil
                        :vargs      [{:msg       "Test message"
                                      :extra-key "extra-value"
                                      :count     5}]
                        :timestamp_ (delay "2024-01-01T00:00:00Z")}
            output     (output-fn log-data)
            parsed     (json/read-str output :key-fn keyword)]
        (assertions
          "Produces valid JSON"
          (string? output) => true
          "Includes the message"
          (:msg parsed) => "Test message"
          "Includes extra keys from the map"
          (:extra-key parsed) => "extra-value"
          (:count parsed) => 5
          "Includes standard log metadata"
          (:level parsed) => "info"
          (:ns parsed) => "test.ns"
          (:line parsed) => 42
          (:service parsed) => "test-service"
          "Includes timestamp"
          (:timestamp parsed) => "2024-01-01T00:00:00Z")))

    (component "with string log entry"
      (let [log-data {:level      :warn
                      :?ns-str    "other.ns"
                      :?line      100
                      :?err       nil
                      :vargs      ["Simple" "string" "message"]
                      :timestamp_ (delay "2024-01-02T00:00:00Z")}
            output   (output-fn log-data)
            parsed   (json/read-str output :key-fn keyword)]
        (assertions
          "Produces valid JSON"
          (string? output) => true
          "Concatenates vargs into :msg"
          (:msg parsed) => "Simple string message"
          "Includes level"
          (:level parsed) => "warn")))

    (component "with exception"
      (let [ex       (ex-info "Test error" {:error-code 500})
            log-data {:level      :error
                      :?ns-str    "error.ns"
                      :?line      200
                      :?err       ex
                      :vargs      [{:msg "Error occurred"}]
                      :timestamp_ (delay "2024-01-03T00:00:00Z")}
            output   (output-fn log-data)
            parsed   (json/read-str output :key-fn keyword)]
        (assertions
          "Produces valid JSON"
          (string? output) => true
          "Includes error information"
          (contains? parsed :error) => true
          "Error has type"
          (contains? (:error parsed) :type) => true
          "Error has message"
          (:message (:error parsed)) => "Test error"
          "Error has data"
          (get-in parsed [:error :data :error-code]) => 500)))

    (component "with nested exception"
      (let [cause    (ex-info "Root cause" {:root true})
            ex       (ex-info "Wrapper error" {:wrapper true} cause)
            log-data {:level      :error
                      :?ns-str    "error.ns"
                      :?line      300
                      :?err       ex
                      :vargs      ["Error with cause"]
                      :timestamp_ (delay "2024-01-04T00:00:00Z")}
            output   (output-fn log-data)
            parsed   (json/read-str output :key-fn keyword)]
        (assertions
          "Includes cause chain"
          (get-in parsed [:error :cause :message]) => "Root cause"
          (get-in parsed [:error :cause :data :root]) => true)))

    (component "with non-serializable data"
      (let [log-data {:level      :info
                      :?ns-str    "test.ns"
                      :?line      42
                      :?err       nil
                      ;; Object that can't be JSON serialized directly
                      :vargs      [(Object.)]
                      :timestamp_ (delay "2024-01-05T00:00:00Z")}
            output   (output-fn log-data)
            parsed   (json/read-str output :key-fn keyword)]
        (assertions
          "Falls back to string representation"
          (string? output) => true
          (string? (:msg parsed)) => true)))))

(specification "exception->map"
  (let [exception->map #'logging/exception->map]

    (component "simple exception"
      (let [ex     (Exception. "Simple error")
            result (exception->map ex)]
        (assertions
          "Returns a map"
          (map? result) => true
          "Has type"
          (:type result) => "java.lang.Exception"
          "Has message"
          (:message result) => "Simple error")))

    (component "ex-info with data"
      (let [ex     (ex-info "Clojure error" {:key "value" :num 42})
            result (exception->map ex)]
        (assertions
          "Has data from ex-info"
          (get-in result [:data :key]) => "value"
          (get-in result [:data :num]) => 42)))

    (component "nil exception"
      (assertions
        "Returns nil for nil input"
        (exception->map nil) => nil))

    (component "exception with cause"
      (let [cause  (Exception. "Underlying problem")
            ex     (Exception. "Top-level error" cause)
            result (exception->map ex)]
        (assertions
          "Has cause"
          (contains? result :cause) => true
          "Cause has message"
          (get-in result [:cause :message]) => "Underlying problem")))))
