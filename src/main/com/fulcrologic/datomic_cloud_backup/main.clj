(ns com.fulcrologic.datomic-cloud-backup.main
  "CLI entry point for continuous live replication.
   
   Run with:
   ```
   clj -X:replicate \\
     :source-client-config '{:server-type :cloud :region \"us-east-1\" :system \"prod\" :endpoint \"https://...\"}' \\
     :source-db-name '\"my-database\"' \\
     :target-client-config '{:server-type :cloud :region \"us-west-2\" :system \"dr\" :endpoint \"https://...\"}' \\
     :target-db-name '\"my-database-replica\"'
   ```"
  (:require
    [clojure.core.async :as async]
    [com.fulcrologic.datomic-cloud-backup.continuous-restore :as cr]
    [com.fulcrologic.datomic-cloud-backup.live-transaction-store :as lts]
    [com.fulcrologic.datomic-cloud-backup.logging :as logging]
    [datomic.client.api :as d]
    [taoensso.timbre :as log]))

(defn continuous-replicate
  "Entry point for clj -X. Runs continuous live replication from source to target database.
   
   Required parameters:
   - :source-client-config - Map for d/client (source Datomic system)
   - :source-db-name - Database name on source (string)
   - :target-client-config - Map for d/client (target Datomic system)
   - :target-db-name - Database name on target (string)
   
   Optional parameters:
   - :segment-size - Transactions per segment (default 1000)
   - :poll-interval-ms - Polling interval when idle (default 5000)
   - :prefetch-buffer - Pre-fetch buffer size (default 5)
   - :max-retry-delay-ms - Maximum backoff delay on errors (default 300000)
   - :log-level - Timbre log level keyword (default :info)
   - :blacklist - Set of attribute keywords to exclude from restore
   - :verify? - Enable ID verification (default true)
   
   Example:
   ```
   clj -X:replicate \\
     :source-client-config '{:server-type :cloud :region \"us-east-1\" :system \"prod\" :endpoint \"https://...\"}' \\
     :source-db-name '\"production-db\"' \\
     :target-client-config '{:server-type :cloud :region \"us-west-2\" :system \"backup\" :endpoint \"https://...\"}' \\
     :target-db-name '\"production-db-replica\"' \\
     :segment-size 1000 \\
     :poll-interval-ms 5000
   ```
   
   The replication will:
   1. Connect to source and target databases
   2. Create target database if it doesn't exist
   3. Start continuous restore from source to target
   4. Run until interrupted (SIGTERM/SIGINT)
   
   Logs are JSON formatted for CloudWatch ingestion."
  [{:keys [source-client-config source-db-name
           target-client-config target-db-name
           segment-size poll-interval-ms prefetch-buffer max-retry-delay-ms
           log-level blacklist verify?]
    :or   {segment-size       1000
           poll-interval-ms   5000
           prefetch-buffer    5
           max-retry-delay-ms 300000
           log-level          :info
           blacklist          #{}
           verify?            true}}]
  ;; Configure JSON logging for CloudWatch
  (logging/configure-json-logging! {:min-level    log-level
                                    :service-name "datomic-replication"})

  ;; Validate required parameters
  (when-not source-client-config
    (log/error {:msg "Missing required parameter :source-client-config"})
    (System/exit 1))
  (when-not source-db-name
    (log/error {:msg "Missing required parameter :source-db-name"})
    (System/exit 1))
  (when-not target-client-config
    (log/error {:msg "Missing required parameter :target-client-config"})
    (System/exit 1))
  (when-not target-db-name
    (log/error {:msg "Missing required parameter :target-db-name"})
    (System/exit 1))

  (log/info {:msg                "Starting continuous replication"
             :source-db          source-db-name
             :target-db          target-db-name
             :segment-size       segment-size
             :poll-interval-ms   poll-interval-ms
             :prefetch-buffer    prefetch-buffer
             :max-retry-delay-ms max-retry-delay-ms})

  (try
    ;; Connect to source
    (log/info {:msg "Connecting to source database" :db source-db-name})
    (let [source-client (d/client source-client-config)
          source-conn   (d/connect source-client {:db-name source-db-name})]
      (log/info {:msg "Connected to source" :db source-db-name})

      ;; Connect to target (create if needed)
      (log/info {:msg "Connecting to target database" :db target-db-name})
      (let [target-client (d/client target-client-config)
            _             (d/create-database target-client {:db-name target-db-name})
            target-conn   (d/connect target-client {:db-name target-db-name})]
        (log/info {:msg "Connected to target" :db target-db-name})

        ;; Create live transaction store
        (let [store    (lts/new-live-store source-conn {:segment-size segment-size})
              running? (volatile! true)]

          ;; Set up shutdown hook
          (.addShutdownHook
            (Runtime/getRuntime)
            (Thread.
              (fn []
                (log/info {:msg "Shutdown signal received, stopping replication"})
                (vreset! running? false))))

          ;; Start continuous restore
          (log/info {:msg "Starting continuous restore loop"})
          (let [result-ch (cr/continuous-restore!
                            source-db-name
                            target-conn
                            store
                            running?
                            {:poll-interval-ms   poll-interval-ms
                             :prefetch-buffer    prefetch-buffer
                             :max-retry-delay-ms max-retry-delay-ms
                             :blacklist          blacklist
                             :verify?            verify?})]

            ;; Block until done
            (let [result (async/<!! result-ch)]
              (log/info {:msg "Replication stopped" :result result})
              (System/exit 0))))))

    (catch Throwable e
      (log/error e {:msg   "Fatal error during replication"
                    :error (ex-message e)
                    :data  (ex-data e)})
      (System/exit 1))))
