# Indexer Core

`indexer-core` provides the core building blocks for VeChainThor indexers. It supports fast log-based catch-up, full block processing when required, ABI and business-event decoding, dependency-aware execution, and rollback-safe reprocessing after reorgs.

## Requirements

- Java 21
- Access to a Thor API endpoint
- A persistence layer that can store the last synced block and roll back on reorgs

## Installation

Gradle Kotlin DSL:

```kotlin
dependencies {
    implementation("org.vechain:indexer-core:8.0.3")
}
```

Gradle Groovy DSL:

```groovy
dependencies {
    implementation 'org.vechain:indexer-core:8.0.3'
}
```

## Quick Start

```kotlin
class MyProcessor(
    private val repository: MyRepository,
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

val thorClient = DefaultThorClient("https://mainnet.vechain.org")

val indexer =
    IndexerFactory()
        .name("example-indexer")
        .thorClient(thorClient)
        .processor(MyProcessor(repository))
        .startBlock(19_000_000)
        .build()

val job =
    IndexerRunner.launch(
        scope = scope,
        thorClient = thorClient,
        indexers = listOf(indexer),
    )
```

## Choosing a Mode

- Default `LogsIndexer`: best when you want the fastest catch-up path and only need decoded events or transfers.
- `includeFullBlock()`: use when you need full block contents, reverted transactions, gas metadata, or clause inspection results.
- `dependsOn(...)`: use when one indexer must finish a block before another processes that same block.

Processors should handle both `IndexingResult.LogResult` and `IndexingResult.BlockResult`.

## Documentation

The detailed documentation lives in [`docs/README.md`](docs/README.md) and is the canonical source of truth for library behavior.

- [`docs/README.md`](docs/README.md): documentation index and navigation
- [`docs/IndexerOverview.md`](docs/IndexerOverview.md): runtime model, lifecycle, runner behavior
- [`docs/LogsIndexerOverview.md`](docs/LogsIndexerOverview.md): log-based indexing and fast sync
- [`docs/EventsAndABIHandling.md`](docs/EventsAndABIHandling.md): ABI loading and event decoding
- [`docs/BusinessEvents.md`](docs/BusinessEvents.md): business event definitions and matching
- [`docs/MIGRATION-8.0.0.md`](docs/MIGRATION-8.0.0.md): 7.x to 8.x migration guide

## Documentation Model

To reduce drift:

- keep `README.md` short and focused on overview plus quick start
- keep detailed technical docs in `docs/`
- treat the repo docs as the canonical source
- use Confluence as a landing page that links to the repo docs instead of duplicating them manually
