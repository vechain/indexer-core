# Indexer Core

This library provides the core functionality for building an indexer for VechainThor. It supports running multiple indexers concurrently with automatic dependency management and parallel processing.

## Features

- **Parallel Processing**: Multiple indexers process blocks concurrently for maximum performance
- **Dependency Management**: Automatically orders indexers based on their dependencies using topological sorting
- **Retry Logic**: Built-in retry mechanisms for initialization, sync, and block processing
- **Block Buffering**: Configurable buffering to optimize throughput
- **Group Coordination**: Indexers with dependencies are organized into processing groups that maintain proper ordering

## Example usage

```kotlin
@Configuration
open class IndexerConfig() {

    @Bean
    open fun myPruner(): Pruner = PrunerService()

    @Bean
    open fun blockIndexer(myPruner: Pruner): Indexer =
        IndexerFactory()
            .name("BlockIndexer")
            .thorClient("https://mainnet.vechain.org", Pair("X-Project-Id", "my-indexer"))
            .processor(blockProcessor)
            .pruner(myPruner)
            .startBlock(0)
            .syncLoggerInterval(1000)
            .abis("/abis")
            .businessEvents("/business-events", "/abis")
            .includeVetTransfers()
            .build()

    @Bean
    open fun transactionIndexer(blockIndexer: Indexer): Indexer =
        IndexerFactory()
            .name("TransactionIndexer")
            .thorClient("https://mainnet.vechain.org")
            .processor(txProcessor)
            .dependsOn(blockIndexer)  // Ensures BlockIndexer processes blocks first
            .startBlock(0)
            .build()

    @Bean
    open fun indexerRunner(
        scope: CoroutineScope,
        thorClient: ThorClient,
        blockIndexer: Indexer,
        transactionIndexer: Indexer
    ): Job {
        return IndexerRunner.launch(
            scope = scope,
            thorClient = thorClient,
            indexers = listOf(blockIndexer, transactionIndexer),
            blockBatchSize = 10  // Buffer up to 10 blocks per group
        )
    }
}
```

The `IndexerFactory` can be used to configure your indexer. The only required parameters are the `name`, `thorClient` and the `processor`.
For details of the available configuration options, see the comments in the `IndexerFactory` class.


## IndexerProcessor Implementation

An example of an `IndexerProcessor` implementation:

```kotlin
@Component
open class MyProcessor(
  private val myService: Service,
) : IndexerProcessor {
    override fun process(entry: IndexingResult) {
        if (entry.events().isEmpty()) {
            return
        }
        myService.processEvents(entry.events())
    }

    override fun rollback(blockNumber: Long) {
        myService.rollback(blockNumber)
    }

    override fun getLastSyncedBlock(): BlockIdentifier? {
      myService.getLatestRecord()?.let {
        return BlockIdentifier(number = it.blockNumber, id = it.blockId)
      }
      logger.info("No records found in repository, returning null")
      return null
    }
}
```

## Architecture

### IndexerRunner

The `IndexerRunner` coordinates multiple indexers:

1. **Initialization Phase**: All indexers are initialized and fast-synced concurrently
2. **Execution Phase**: Indexers are organized into dependency groups and process blocks in parallel
   - Groups are determined by topological sorting based on `dependsOn` relationships
   - Within each group, indexers process the same block **sequentially** in list order
   - Different groups can work on different blocks simultaneously (e.g., Group 2 on block N+1 while Group 1 on block N+2)

### Dependency Management

The `IndexerOrderUtils` provides topological sorting to determine the correct processing order:

```kotlin
val indexer1 = createIndexer("Base")
val indexer2 = createIndexer("DependentA", dependsOn = indexer1)
val indexer3 = createIndexer("DependentB", dependsOn = indexer1)
val indexer4 = createIndexer("FinalIndexer", dependsOn = indexer2)

// Results in groups: [[Base], [DependentA, DependentB], [FinalIndexer]]
// DependentA and DependentB can process blocks in parallel
```

## Java compatibility

When using the indexer-core package in a Java project, prefer using `startInCoroutine()` to start your indexer implementation,
as the regular `start()` is a suspend function for which the caller has to supply a coroutine scope.
Otherwise, the package is Java compatible as is.
