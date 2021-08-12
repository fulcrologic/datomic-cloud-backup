(ns com.fulcrologic.datomic-cloud-backup.protocols)

(defprotocol BackupStore
  (saved-segment-info [this dbname]
    "Returns a map containing info about the saved transaction groups for dbname (a keyword). This is a vector of
     maps containing {:start-t a :end-t b} for all saved groups, sorted by :start-t.
     There should be no gaps between the end-t of one and the start-t of the next.")
  (save-transactions! [this dbname transaction-group]
    "Save the given `transaction-group` as the transaction history. The group includes :start-t and :end-t for
     dbname (a keyword). This data must be nippy and transit-compatible EDN. A transaction group is a map
     that contains the info about the database as-of `end-t` along with all of the changes that
     happened from start-t to end-t.")
  (load-transaction-group [this dbname start-t]
    "Load a transaction group for dbname. `start-t` can be 0 to indicate the first group. The return value is a map containing:

      `:start-t` - The original transaction number that this group started at.
      `:end-t` - The original transaction number that this group ended at (inclusive).
      `:transaction-group` - The original data saved via `save-transaction!`.

      ONLY returns a transaction group if `start-t` is EXACTLY right. The next desirable transaction group is always
      `(inc end-t)` of the prior one. Transaction groups can vary in size, so do not assume you can guess this value."))

(defprotocol IDMapper
  "A durable map-like storage for ID mappings. This is used to keep track of what ID is used in
   a new database (during restore) so that references to if from the old database can be corrected."
  (store-id-mappings! [this dbname source-id->target-id]
    "Accumulate the given mappings (from source ID to target ID, all longs) into the ID Mapper")
  (resolve-id [this dbname source-id]
    "Returns the target-id that is associated with the given source-id in the mapper"))
