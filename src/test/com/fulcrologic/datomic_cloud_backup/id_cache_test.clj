(ns com.fulcrologic.datomic-cloud-backup.id-cache-test
  (:require
    [com.fulcrologic.datomic-cloud-backup.id-cache :as idc]
    [fulcro-spec.core :refer [=> assertions component specification]]))

(specification "LRU Cache basic operations"
  (let [cache (idc/new-lru-cache)]
    (assertions
      "Starts empty"
      (idc/cache-size cache) => 0

      "lookup returns nil for unknown keys"
      (idc/lookup cache 100) => nil))

  (let [cache (idc/new-lru-cache)]
    (idc/store! cache 100 999)
    (assertions
      "After storing, lookup returns the value"
      (idc/lookup cache 100) => 999

      "Cache size is 1"
      (idc/cache-size cache) => 1)))

(specification "LRU Cache stats tracking"
  (let [cache (idc/new-lru-cache)]
    ;; Store some entries
    (idc/store! cache 100 1001)
    (idc/store! cache 200 1002)

    ;; Perform various lookups
    (idc/lookup cache 100)                                  ; hit
    (idc/lookup cache 100)                                  ; hit
    (idc/lookup cache 200)                                  ; hit
    (idc/lookup cache 150)                                  ; miss
    (idc/lookup cache 300)                                  ; miss

    (let [{:keys [hits misses size]} (idc/cache-stats cache)]
      (assertions
        "Tracks cache hits"
        hits => 3

        "Tracks cache misses"
        misses => 2

        "Reports cache size"
        size => 2))))

(specification "LRU Cache size limits"
  (component "Cache respects max-size"
    (let [cache (idc/new-lru-cache {:max-size 5})]
      (assertions
        "Reports configured max size"
        (idc/max-cache-size cache) => 5

        "Starts empty"
        (idc/cache-size cache) => 0)

      ;; Fill the cache to capacity
      (idc/store! cache 1 1001)
      (idc/store! cache 2 1002)
      (idc/store! cache 3 1003)
      (idc/store! cache 4 1004)
      (idc/store! cache 5 1005)

      (assertions
        "Cache grows up to max size"
        (idc/cache-size cache) => 5

        "All entries are present"
        (idc/lookup cache 1) => 1001
        (idc/lookup cache 2) => 1002
        (idc/lookup cache 3) => 1003
        (idc/lookup cache 4) => 1004
        (idc/lookup cache 5) => 1005))))

(specification "LRU eviction behavior"
  (component "Basic eviction"
    (let [cache (idc/new-lru-cache {:max-size 3})]
      ;; Fill the cache
      (idc/store! cache 1 1001)
      (idc/store! cache 2 1002)
      (idc/store! cache 3 1003)

      (assertions
        "Cache is at capacity"
        (idc/cache-size cache) => 3)

      ;; Add a 4th entry - should evict the least recently used (1)
      (idc/store! cache 4 1004)

      (assertions
        "Cache size doesn't exceed max"
        (idc/cache-size cache) => 3

        "Least recently used entry (1) was evicted"
        (idc/lookup cache 1) => nil

        "Other entries still present"
        (idc/lookup cache 2) => 1002
        (idc/lookup cache 3) => 1003
        (idc/lookup cache 4) => 1004)))

  (component "Access-order LRU"
    (let [cache (idc/new-lru-cache {:max-size 3})]
      ;; Fill the cache
      (idc/store! cache 1 1001)
      (idc/store! cache 2 1002)
      (idc/store! cache 3 1003)

      ;; Access entry 1 to make it recently used
      (idc/lookup cache 1)

      ;; Add entry 4 - should evict 2 (now least recently used)
      (idc/store! cache 4 1004)

      (assertions
        "Recently accessed entry (1) survives eviction"
        (idc/lookup cache 1) => 1001

        "Least recently used (2) was evicted"
        (idc/lookup cache 2) => nil

        "Other entries still present"
        (idc/lookup cache 3) => 1003
        (idc/lookup cache 4) => 1004)))

  (component "Eviction stats tracking"
    (let [cache (idc/new-lru-cache {:max-size 2})]
      (idc/store! cache 1 1001)
      (idc/store! cache 2 1002)

      (let [stats-before (idc/cache-stats cache)]
        (assertions
          "No evictions yet"
          (:evictions stats-before) => 0))

      ;; This should cause an eviction
      (idc/store! cache 3 1003)

      (let [stats-after (idc/cache-stats cache)]
        (assertions
          "Eviction is tracked"
          (:evictions stats-after) => 1))

      ;; Another eviction
      (idc/store! cache 4 1004)

      (let [stats-final (idc/cache-stats cache)]
        (assertions
          "Multiple evictions are counted"
          (:evictions stats-final) => 2)))))

(specification "LRU Cache update existing key"
  (let [cache (idc/new-lru-cache {:max-size 3})]
    (idc/store! cache 1 1001)
    (idc/store! cache 2 1002)
    (idc/store! cache 3 1003)

    ;; Update existing key
    (idc/store! cache 2 9999)

    (assertions
      "Updating existing key doesn't increase size"
      (idc/cache-size cache) => 3

      "Value is updated"
      (idc/lookup cache 2) => 9999

      "Other entries unaffected"
      (idc/lookup cache 1) => 1001
      (idc/lookup cache 3) => 1003)))
