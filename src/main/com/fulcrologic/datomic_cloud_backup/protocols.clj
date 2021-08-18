(ns com.fulcrologic.datomic-cloud-backup.protocols)

(defprotocol BackupStore
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

      `:info` - Information about the database at the time of the backup (e.g. existing schema)
      `:start-t` - The original transaction number that this group started at.
      `:end-t` - The original transaction number that this group ended at (inclusive).
      `:transactions` - The original data saved via `save-transaction!`.

      ONLY returns a transaction group if `start-t` (and, if supplied, end-t) is/are EXACTLY right. The next desirable transaction group is always
      `(inc end-t)` of the prior one. Transaction groups can vary in size, so do not assume you can guess this value.
      See `last-segment-info` for an optimal way to know what the last segment is, and `save-segment-info` to get a list of them all."))

(defprotocol IDMapper
  "A durable map-like storage for ID mappings. This is used to keep track of what ID is used in
   a new database (during restore) so that references to if from the old database can be corrected."
  (store-id-mappings! [this dbname source-id->target-id]
    "Accumulate the given mappings (from source ID to target ID, all longs) into the ID Mapper")
  (resolve-id [this dbname source-id]
    "Returns the target-id that is associated with the given source-id in the mapper"))
