// Package metadata implements the metadata plane: durable broker state that is
// not event data (docs/Storage.md §6, docs/Architecture.md §5).
//
// Phase 1 ships two implementations of ports.MetadataStore:
//
//   - PebbleMetadataStore, a production store backed by Pebble using sync
//     commits for every durable write;
//   - InMemoryMetadataStore, a mutex-guarded store used by tests and the
//     ephemeral CLI mode.
//
// Keys follow the schema in docs/Storage.md §6. Event data never passes
// through this store.
package metadata
