# Agent Guidelines for datomic-cloud-backup

- REPL: Include the :dev alias when running REPLs to get local dependencies.

## Code Style
- **Namespaces**: Use `com.fulcrologic.datomic-cloud-backup.*` prefix
- **Imports**: Group requires by stdlib, deps, project; use `:refer` sparingly
- **Types**: Use Guardrails `>defn` with Malli specs for public functions
- **Protocols**: Define in `protocols.clj`, implement in separate files per store type
- **Error handling**: Use `ex-info` with meaningful data maps, log with timbre

## Architecture
- `protocols.clj`: TransactionStore protocol definition
- `cloning.clj`: Core backup/restore logic with Datomic client API
- `*_store.clj`: Protocol implementations (filesystem, S3, RAM)
- Tests use datomic dev-local with `:mem` storage
