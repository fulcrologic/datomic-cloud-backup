(ns com.fulcrologic.datomic-cloud-backup.continuous-restore
  "Continuous restore functionality that polls a TransactionStore and applies
   new segments to a target database as they become available.
   
   This namespace provides background restore that:
   - Pre-fetches segments ahead of the consumer using core.async
   - Uses exponential backoff on errors
   - Supports graceful shutdown via a running? flag
   - Logs structured maps for CloudWatch integration"
  (:require
    [clojure.core.async :as async :refer [go go-loop >! <! <!! chan close!]]
    [com.fulcrologic.datomic-cloud-backup.cloning :as cloning]
    [com.fulcrologic.datomic-cloud-backup.protocols :as dcbp]
    [com.fulcrologic.guardrails.core :refer [>defn =>]]
    [datomic.client.api :as d]
    [taoensso.timbre :as log]))

;; =============================================================================
;; Exponential Backoff
;; =============================================================================

(def ^:const initial-retry-delay-ms 1000)

(defn- calculate-backoff
  "Calculate exponential backoff delay, doubling each time up to max-delay."
  [current-delay max-delay]
  (min (* 2 current-delay) max-delay))

;; =============================================================================
;; Pre-fetch Producer
;; =============================================================================

(defn- fetch-next-segment
  "Attempt to fetch the next segment from the store.
   Returns either:
   - {:segment <segment-data>} with the loaded segment
   - {:caught-up? true :current-t t} when nothing new is available
   - {:error e :current-t t} on exception"
  [transaction-store dbname next-start-t-atom]
  (try
    (let [desired-start-t @next-start-t-atom
          {:keys [end-t] :as last-info} (dcbp/last-segment-info transaction-store dbname)]
      (if (or (nil? last-info) (< end-t desired-start-t))
        ;; Nothing new available - the last stored segment ends before where we want to start
        {:caught-up? true :current-t desired-start-t}
        ;; There's potentially a segment to fetch
        (let [segment-start (cloning/find-segment-start-t transaction-store dbname desired-start-t)]
          (if (nil? segment-start)
            {:caught-up? true :current-t desired-start-t}
            ;; Load the segment
            (let [segment (dcbp/load-transaction-group transaction-store dbname segment-start)]
              (if segment
                {:segment segment}
                {:caught-up? true :current-t desired-start-t}))))))
    (catch Throwable e
      {:error e :current-t @next-start-t-atom})))

(defn- start-prefetch-producer!
  "Starts a producer that pre-fetches transaction segments into a buffered channel.
   
   Returns a map with:
   - :channel - The channel to read segments from
   - :stop! - Function to stop the producer
   
   The producer fetches segments ahead of where restore currently is, providing
   them via the channel. It uses backpressure naturally - blocks when buffer is full.
   
   Each item on the channel is either:
   - A segment map from load-transaction-group (with :start-t, :end-t, :transactions, etc.)
   - A map with :caught-up? true when there's nothing new to fetch
   - A map with :error and the exception when fetching fails"
  [transaction-store dbname next-start-t-atom running? {:keys [buffer-size]
                                                        :or   {buffer-size 5}}]
  (let [segment-chan (chan buffer-size)
        producer-running? (volatile! true)]
    ;; Start the producer go-loop
    (go-loop []
      (if-not (and @running? @producer-running?)
        ;; Stop the loop - don't close channel here, stop! will do it
        nil
        ;; Fetch and process
        (let [result (fetch-next-segment transaction-store dbname next-start-t-atom)]
          (cond
            ;; Got a segment
            (:segment result)
            (let [segment (:segment result)]
              ;; Update the atom to point to next segment
              (reset! next-start-t-atom (inc (:end-t segment)))
              ;; Put segment on channel (blocks if buffer full)
              (>! segment-chan segment)
              (recur))
            
            ;; Caught up - signal and wait briefly
            (:caught-up? result)
            (do
              (>! segment-chan result)
              (<! (async/timeout 100))
              (recur))
            
            ;; Error - signal and wait longer
            (:error result)
            (do
              (>! segment-chan result)
              (<! (async/timeout 1000))
              (recur))
            
            ;; Unexpected - just continue
            :else
            (do
              (<! (async/timeout 100))
              (recur))))))
    {:channel segment-chan
     :stop!   (fn []
                (vreset! producer-running? false)
                (close! segment-chan))}))

;; =============================================================================
;; Segment Processing
;; =============================================================================

(defn- get-last-restored-t
  "Get the last source transaction that was restored to the target database."
  [target-conn]
  (let [db (d/db target-conn)]
    (or (::cloning/last-source-transaction 
          (d/pull db [::cloning/last-source-transaction] ::cloning/last-source-transaction))
        0)))

(defn- process-segment!
  "Process a single segment, restoring it to the target database.
   
   Returns:
   - {:success true :end-t n} on success
   - {:success false :error e} on failure"
  [source-database-name target-conn transaction-store segment options]
  (let [start-time (System/currentTimeMillis)
        {:keys [start-t end-t]} segment
        result (try
                 (let [restore-result (cloning/restore-segment! 
                                        source-database-name 
                                        target-conn 
                                        ;; Create a minimal store that just returns this segment
                                        ;; This is a bit of a hack but avoids duplicating restore logic
                                        (reify dcbp/TransactionStore
                                          (last-segment-info [_ _] {:start-t start-t :end-t end-t})
                                          (saved-segment-info [_ _] [{:start-t start-t :end-t end-t}])
                                          (load-transaction-group [_ _ _] segment)
                                          (load-transaction-group [_ _ _ _] segment)
                                          (save-transactions! [_ _ _] nil))
                                        options)]
                   (case restore-result
                     :restored-segment {:success true :end-t end-t}
                     :nothing-new-available {:success true :end-t end-t :nothing-new? true}
                     :transaction-failed! {:success false :error (ex-info "Transaction failed" {:result restore-result})}
                     :partial-segment {:success false :error (ex-info "Partial segment" {:result restore-result})}
                     {:success false :error (ex-info "Unknown result" {:result restore-result})}))
                 (catch Throwable e
                   {:success false :error e}))
        elapsed (- (System/currentTimeMillis) start-time)]
    (if (:success result)
      (let [current-t (get-last-restored-t target-conn)
            {:keys [end-t] :as source-last} (dcbp/last-segment-info transaction-store source-database-name)
            lag (when end-t (- end-t current-t))]
        (when-not (:nothing-new? result)
          (log/info {:msg           "Segment restored"
                     :db            source-database-name
                     :segment-start start-t
                     :segment-end   (:end-t segment)
                     :duration-ms   elapsed
                     :lag-t         lag}))
        (assoc result :duration-ms elapsed :lag-t lag))
      result)))

;; =============================================================================
;; Main Continuous Restore Function  
;; =============================================================================

(>defn continuous-restore!
  "Continuously restore from a TransactionStore with polling.
   
   Arguments:
   - source-database-name: Name used for this backup/restore
   - target-conn: Connection to target database
   - transaction-store: A TransactionStore implementation
   - running?: A derefable (e.g., volatile!) - set to false to stop
   - options map:
     - :poll-interval-ms - Sleep time when caught up (default 5000)
     - :prefetch-buffer - Number of segments to pre-fetch (default 5)
     - :max-retry-delay-ms - Maximum backoff delay on errors (default 300000 = 5 min)
     - :blacklist - Set of attributes to exclude from restore
     - :rewrite - Map of attribute -> rewrite fn
     - :verify? - Enable ID verification (default true)
   
   Returns: A channel that receives the final result when stopped
   
   The restore runs in background threads. It:
   1. Starts a prefetch producer to load segments ahead
   2. Consumes segments and restores them
   3. When caught up (no new segments), sleeps poll-interval-ms
   4. On errors, uses exponential backoff (1s -> 2s -> 4s -> ... -> max-retry-delay-ms)
   5. Stops when @running? is false"
  [source-database-name target-conn transaction-store running? options]
  [string? any? ::dcbp/store any? map? => any?]
  (let [{:keys [poll-interval-ms prefetch-buffer max-retry-delay-ms blacklist rewrite verify?]
         :or   {poll-interval-ms   5000
                prefetch-buffer    5
                max-retry-delay-ms 300000
                blacklist          #{}
                verify?            true}} options
        restore-options {:blacklist blacklist
                         :rewrite   rewrite
                         :verify?   verify?}
        result-chan     (chan 1)
        
        ;; Determine where to start based on target database state
        last-restored-t (get-last-restored-t target-conn)
        desired-start-t (if (pos? last-restored-t) (inc last-restored-t) 1)
        next-start-t    (atom desired-start-t)
        
        ;; Start the prefetch producer
        {:keys [channel stop!]} (start-prefetch-producer! 
                                  transaction-store 
                                  source-database-name 
                                  next-start-t 
                                  running?
                                  {:buffer-size prefetch-buffer})]
    
    ;; Ensure restore schema exists
    (when (< desired-start-t 2)
      (cloning/ensure-restore-schema! target-conn))
    
    (log/info {:msg       "Starting continuous restore"
               :db        source-database-name
               :start-t   desired-start-t
               :options   (select-keys options [:poll-interval-ms :prefetch-buffer :max-retry-delay-ms])})
    
    ;; Start the consumer go-loop
    (go-loop [retry-delay initial-retry-delay-ms
              segments-restored 0]
      (if-not @running?
        ;; Shutting down
        (do
          (stop!)
          (log/info {:msg               "Continuous restore stopped"
                     :db                source-database-name
                     :segments-restored segments-restored})
          (>! result-chan {:status            :stopped
                           :segments-restored segments-restored})
          (close! result-chan))
        ;; Process next item from channel
        (let [item (<! channel)]
          (cond
            ;; Channel closed
            (nil? item)
            (do
              (log/info {:msg               "Prefetch channel closed"
                         :db                source-database-name
                         :segments-restored segments-restored})
              (>! result-chan {:status            :channel-closed
                               :segments-restored segments-restored})
              (close! result-chan))
            
            ;; Caught up - poll after delay
            (:caught-up? item)
            (do
              (log/info {:msg              "Caught up, polling"
                         :db               source-database-name
                         :current-t        (:current-t item)
                         :poll-interval-ms poll-interval-ms})
              (<! (async/timeout poll-interval-ms))
              (recur initial-retry-delay-ms segments-restored))
            
            ;; Error from producer
            (:error item)
            (let [e (:error item)]
              (log/error {:msg            "Restore error, retrying"
                          :db             source-database-name
                          :error          (ex-message e)
                          :retry-delay-ms retry-delay})
              (<! (async/timeout retry-delay))
              (recur (calculate-backoff retry-delay max-retry-delay-ms) segments-restored))
            
            ;; Normal segment - process it
            :else
            (let [result (process-segment! source-database-name target-conn transaction-store item restore-options)]
              (if (:success result)
                ;; Success - reset backoff and continue
                (do
                  ;; Update start-t atom to sync with actual progress
                  (reset! next-start-t (inc (get-last-restored-t target-conn)))
                  (recur initial-retry-delay-ms (inc segments-restored)))
                ;; Failure - backoff and retry
                (let [e (:error result)]
                  (log/error {:msg            "Restore error, retrying"
                              :db             source-database-name
                              :error          (ex-message e)
                              :retry-delay-ms retry-delay})
                  (<! (async/timeout retry-delay))
                  (recur (calculate-backoff retry-delay max-retry-delay-ms) segments-restored))))))))
    
    result-chan))

(comment
  ;; Example usage:
  ;; 
  ;; (def running? (volatile! true))
  ;; 
  ;; (def result-chan
  ;;   (continuous-restore!
  ;;     "my-database"
  ;;     target-conn
  ;;     my-transaction-store
  ;;     running?
  ;;     {:poll-interval-ms 5000
  ;;      :prefetch-buffer 5
  ;;      :max-retry-delay-ms 300000}))
  ;; 
  ;; ;; To stop:
  ;; (vreset! running? false)
  ;; 
  ;; ;; Wait for final result:
  ;; (async/<!! result-chan)
  )
