# indexer-core 8.0.0 Migration Guide

## Breaking Changes

There are two breaking changes in 8.0.0:

### 1. `IndexingResult` inner classes renamed

| 7.x | 8.0.0 |
|---|---|
| `IndexingResult.Normal` | `IndexingResult.BlockResult` |
| `IndexingResult.EventsOnly` | `IndexingResult.LogResult` |

**Action**: Update all `when` blocks and type checks in your `IndexerProcessor.process()` implementation:

```kotlin
// Before
override suspend fun process(entry: IndexingResult) {
    when (entry) {
        is IndexingResult.Normal -> { /* ... */ }
        is IndexingResult.EventsOnly -> { /* ... */ }
    }
}

// After
override suspend fun process(entry: IndexingResult) {
    when (entry) {
        is IndexingResult.BlockResult -> { /* ... */ }
        is IndexingResult.LogResult -> { /* ... */ }
    }
}
```

The properties on each class are unchanged — only the names differ.

### 2. Pruner functionality removed entirely

The following APIs no longer exist:

| Removed item | Type |
|---|---|
| `Pruner` | Interface |
| `Status.PRUNING` | Enum value |
| `Indexer.pruner` | Property |
| `IndexerFactory.pruner(Pruner)` | Builder method |
| `IndexerFactory.prunerInterval(Long)` | Builder method |

**Action**: Remove all pruner usage from your factory configuration:

```kotlin
// Before
IndexerFactory()
    .name("my-indexer")
    .thorClient(client)
    .processor(processor)
    .pruner(myPruner)          // remove
    .prunerInterval(5000L)     // remove
    .build()

// After
IndexerFactory()
    .name("my-indexer")
    .thorClient(client)
    .processor(processor)
    .build()
```

**Action**: Delete any classes that implement `Pruner`.

**Action**: If you check for `Status.PRUNING` anywhere (e.g. health checks, monitoring), remove those checks. The valid status values are now: `NOT_INITIALISED`, `INITIALISED`, `FAST_SYNCING`, `SYNCING`, `FULLY_SYNCED`, `SHUT_DOWN`.

**If you still need periodic cleanup**: implement it as a separate scheduled job outside of the indexer library (e.g. a coroutine timer or Spring `@Scheduled` method that runs independently).

## Search patterns to find code that needs updating

```
# Find Pruner references
grep -rn "Pruner" --include="*.kt"
grep -rn "prunerInterval" --include="*.kt"
grep -rn "PRUNING" --include="*.kt"

# Find old IndexingResult names
grep -rn "IndexingResult\.Normal" --include="*.kt"
grep -rn "IndexingResult\.EventsOnly" --include="*.kt"
```

## Quick checklist

- [ ] Update dependency version to `8.0.0`
- [ ] Rename `IndexingResult.Normal` → `IndexingResult.BlockResult`
- [ ] Rename `IndexingResult.EventsOnly` → `IndexingResult.LogResult`
- [ ] Remove `.pruner(...)` calls from `IndexerFactory`
- [ ] Remove `.prunerInterval(...)` calls from `IndexerFactory`
- [ ] Delete `Pruner` implementations
- [ ] Remove `Status.PRUNING` checks
- [ ] Build and run tests
