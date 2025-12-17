(ns com.fulcrologic.datomic-cloud-backup.id-cache
  "LRU cache for long->long mappings with bounded size.
  
  This is a simple LRU cache - it doesn't know anything about Datomic IDs
  or entity indices. That logic belongs in the restore code that uses this cache."
  (:require
    [com.fulcrologic.guardrails.core :refer [>defn =>]])
  (:import
    (java.util LinkedHashMap)))

;; Default max cache size - 1 million entries uses ~48MB
;; (16 bytes per Long key + 16 bytes per Long value + overhead)
(def ^:const default-max-cache-size 1000000)

(defprotocol ILRUCache
  "Protocol for an LRU cache mapping long keys to long values."
  (lookup [this k]
    "Look up value for key k. Returns nil if not found. Marks entry as recently used.")
  (store! [this k v]
    "Store mapping k->v. May evict least-recently-used entry if at capacity.")
  (cache-size [this]
    "Returns the current number of entries in the cache.")
  (max-cache-size [this]
    "Returns the maximum allowed cache size.")
  (cache-stats [this]
    "Returns stats about cache hits/misses/evictions."))

(defn- make-lru-map
  "Create a LinkedHashMap that acts as an LRU cache with the given max size.
   Uses access-order (third param = true) so that gets move entries to the end,
   and removeEldestEntry evicts the least-recently-used entry when size exceeds max."
  ^LinkedHashMap [max-size]
  (proxy [LinkedHashMap] [(int (inc max-size)) (float 0.75) true]
    (removeEldestEntry [_eldest]
      (> (.size ^LinkedHashMap this) (int max-size)))))

(deftype LRUCache [^LinkedHashMap lru-map
                   ^long max-size
                   ^:volatile-mutable hits
                   ^:volatile-mutable misses
                   ^:volatile-mutable evictions]
  ILRUCache
  (lookup [_ k]
    (if-let [result (.get lru-map (Long/valueOf (long k)))]
      (do
        (set! hits (inc hits))
        (long result))
      (do
        (set! misses (inc misses))
        nil)))

  (store! [_ k v]
    (let [size-before (.size lru-map)]
      (.put lru-map (Long/valueOf (long k)) (Long/valueOf (long v)))
      ;; Track evictions: if size didn't grow and we're at capacity, something was evicted
      (when (and (= size-before max-size)
                 (= (.size lru-map) max-size))
        (set! evictions (inc evictions)))))

  (cache-size [_] (.size lru-map))
  
  (max-cache-size [_] max-size)

  (cache-stats [_]
    {:hits           hits
     :misses         misses
     :evictions      evictions
     :size           (.size lru-map)
     :max-size       max-size}))

(defn new-lru-cache
  "Create a new LRU cache for long->long mappings.
   
   Options:
   - :max-size - Maximum number of entries before LRU eviction (default 1,000,000)
   
   Memory usage is approximately 48 bytes per entry."
  ([] (new-lru-cache {}))
  ([{:keys [max-size] :or {max-size default-max-cache-size}}]
   (->LRUCache (make-lru-map max-size) max-size 0 0 0)))
