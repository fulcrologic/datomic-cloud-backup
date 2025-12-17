(ns com.fulcrologic.datomic-cloud-backup.protocols
  (:require [clojure.spec.alpha :as s]))

(defprotocol TransactionStore
  (last-segment-info [this dbname] "Returns `{:start-t a :end-t b}` for the last segment that has been stored. MAY
   return nil indicating that no segments have been backed up, but should NEVER return nil if there are valid
   segments. This is an optimization that prevents you from having to read all of the segment info to figure it out, but it could
   be implemented that way depending on the store structure/performance.")
  (saved-segment-info [this dbname]
    "Returns a map containing info about the saved transaction groups for dbname (a keyword). This is a vector of
     maps containing {:start-t a :end-t b} for all saved groups, sorted by :start-t.
     There should be no gaps between the end-t of one and the start-t of the next.")
  (save-transactions! [this dbname transaction-group]
    "Save the given `transaction-group` as the transaction history. The group includes :start-t and :end-t for
     dbname (a keyword). This data must be nippy and transit-compatible EDN. A transaction group is a map
     that contains the info about the database as-of `end-t` along with all of the changes that
     happened from start-t to end-t.")
  (load-transaction-group [this dbname start-t] [this dbname start-t end-t]
    "Load a transaction group for dbname. `start-t` can be 0 to indicate the first group. If `end-t` is supplied then
     it is trusted (and can cause failures if it is wrong), but if not supplied it will be derived from the first segment
     found that has `start-t`. The return value is a map containing:

     ```
     {:refs         #{...} ; set of all attribute keys that have a ref type
      :id->attr     {id :k} ; map from :db/id in original database to db ident keyword
      :transactions [...] ; sequence of transactions from the log
      :start-t      actual-start ; starting t of the transactions in this group
      :end-t        actual-end}  ; final t of the transactions in this group
     ```

      ONLY returns a transaction group if `start-t` (and, if supplied, end-t) is/are EXACTLY right. The next desirable transaction group is always
      `(inc end-t)` of the prior one. Transaction groups can vary in size, so do not assume you can guess this value.
      See `last-segment-info` for an optimal way to know what the last segment is, and `save-segment-info` to get a list of them all."))

(s/def ::store #(satisfies? TransactionStore %))