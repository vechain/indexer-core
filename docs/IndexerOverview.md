# Indexer Overview

`indexer-core` exposes a small set of runtime primitives:

- `IndexerProcessor`: your persistence boundary
- `IndexerFactory`: builds configured indexers
- `IndexerRunner`: starts and coordinates them
- `Indexer`: the runtime interface implemented by `BlockIndexer` and `LogsIndexer`

In normal usage you implement `IndexerProcessor`, build indexers with `IndexerFactory`, and run them through `IndexerRunner.launch(...)`.

## Core Types

### `IndexerProcessor`

`IndexerProcessor` is the main integration point for application code.

```kotlin
interface IndexerProcessor {
    fun getLastSyncedBlock(): BlockIdentifier?
    fun rollback(blockNumber: Long)
    suspend fun process(entry: IndexingResult)
}
```

Responsibilities:

- return the last successfully persisted block
- roll back persisted state when the library detects a reorg or re-initialises from a safe point
- handle the indexing payload emitted by the runtime

### `IndexingResult`

The processor receives one of two result types:

- `IndexingResult.LogResult`
  - produced by `LogsIndexer`
  - contains `endBlock`, decoded `events`, and the current `status`
- `IndexingResult.BlockResult`
  - produced by `BlockIndexer`
  - contains the full `block`, decoded `events`, optional `callResults`, and the current `status`

### `Indexer`

`Indexer` is the runtime contract used internally by the runner. The built-in implementations are:

- `LogsIndexer`: default factory output, optimized for fast log-based sync
- `BlockIndexer`: used when full block access or dependency ordering is required

You usually do not implement `Indexer` directly unless you are extending the library itself.

## Typical Setup

```kotlin
class TransfersProcessor(
    private val repository: TransfersRepository,
) : IndexerProcessor {
    override fun getLastSyncedBlock(): BlockIdentifier? = repository.getLastSyncedBlock()

    override fun rollback(blockNumber: Long) {
        repository.rollbackFrom(blockNumber)
    }

    override suspend fun process(entry: IndexingResult) {
        when (entry) {
            is IndexingResult.LogResult -> repository.storeEvents(entry.endBlock, entry.events)
            is IndexingResult.BlockResult ->
                repository.storeBlock(entry.block, entry.events, entry.callResults)
        }
    }
}
```

```kotlin
val thorClient = DefaultThorClient("https://mainnet.vechain.org")

val indexer =
    IndexerFactory()
        .name("transfers")
        .thorClient(thorClient)
        .processor(TransfersProcessor(repository))
        .startBlock(19_000_000)
        .build()
```

```kotlin
val job =
    IndexerRunner.launch(
        scope = scope,
        thorClient = thorClient,
        indexers = listOf(indexer),
    )
```

## Lifecycle

### Initialisation

When an indexer is initialised:

1. The processor's `getLastSyncedBlock()` is queried.
2. The runtime rolls back from that block number if needed.
3. The current block pointer is restored and the indexer moves to `INITIALISED`.

This rollback-on-start behavior is intentional. It lets consumers rebuild the latest processed block from a known safe point.

### Processing

For each block or log batch, the runtime:

1. validates the expected block position
2. updates sync state (`SYNCING` or `FULLY_SYNCED`)
3. checks for reorgs
4. builds an `IndexingResult`
5. calls `IndexerProcessor.process(...)`

### Reorg Handling

Reorg handling is built into the runtime:

- block-based indexers compare the previous processed block ID against the next canonical block
- if a mismatch is detected, a `ReorgException` is raised
- `IndexerRunner` catches that exception, re-initialises indexers, and resumes from the rolled-back state

Your processor only needs to provide deterministic rollback logic.

## Status Values

The runtime can emit these states:

- `NOT_INITIALISED`
- `INITIALISED`
- `FAST_SYNCING`
- `SYNCING`
- `FULLY_SYNCED`
- `SHUT_DOWN`

## Dependency Ordering

Indexers can depend on other indexers through `IndexerFactory.dependsOn(...)`.

```kotlin
val baseIndexer =
    IndexerFactory()
        .name("base")
        .thorClient(thorClient)
        .processor(baseProcessor)
        .build()

val dependentIndexer =
    IndexerFactory()
        .name("dependent")
        .thorClient(thorClient)
        .processor(dependentProcessor)
        .dependsOn(baseIndexer)
        .build()
```

Important behavior:

- dependencies must be included in the same `IndexerRunner.launch(...)` call
- circular dependency chains are rejected
- dependent indexers are executed after their parent for the same block

## Runner Behaviour

`IndexerRunner` coordinates all configured indexers:

1. fast-syncable indexers are initialised and fast-synced concurrently
2. independent non-fast-syncable indexers may run while that fast sync is in progress
3. steady-state execution groups are formed from dependency chains
4. groups can be reshuffled based on block-number proximity for better throughput
5. within a group, indexers run in topological order
6. across groups, prepared blocks are distributed concurrently

The public entry point is:

```kotlin
IndexerRunner.launch(
    scope = scope,
    thorClient = thorClient,
    indexers = indexers,
    blockBatchSize = 1,
    proximityThreshold = 500_000L,
    reshuffleInterval = 15.minutes,
)
```

## When to Use Which Mode

Use a default factory-built `LogsIndexer` when you only need decoded events or VET transfers and want the fastest catch-up path.

Use `includeFullBlock()` when you need:

- full block contents
- reverted transaction visibility
- gas metadata
- clause inspection results from `callDataClauses(...)`

See [`LogsIndexerOverview.md`](./LogsIndexerOverview.md) for the log-based path and [`EventsAndABIHandling.md`](./EventsAndABIHandling.md) for event decoding.
