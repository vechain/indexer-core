# Indexer Core

This library provides the core functionality for building an indexer for a VechainThor. Here is a simple example of how to use it:

## Example usage

```kotlin
@Configuration
open class Config() {

    @Bean
    open fun myPruner(): Pruner = PrunerService()

    @Bean
    open fun myIndexer(
        myProcessor: IndexerProcessor,
        startBlock: Long,
        syncLogInterval: Long,
        syncBlockBatchSize: Long,
        myPruner: Pruner,
    ): Indexer =
        IndexerFactory()
            .name("MyIndexer")
            .thorClient("https://mainnet.vechain.org")
            .processor(myProcessor)
            .pruner(myPruner)
            .startBlock(startBlock)
            .syncLoggerInterval(syncLogInterval)
            .abis(FileUtils.getJsonFilePaths("/abis"))
            .businessEvents(FileUtils.getJsonFilePaths("/business-events"))
            .excludeVetTransfers()
            .build()
}
```

The `IndexerFactory` can be used to configure your indexer. The only required parameters are the `name`, `thorClient` and the `processor`.
For details of the available configuration options, see the comments in the `IndexerFactory` class.

An example of an `IndexerProcessor` implementation is as follows:

```kotlin
@Component
open class MyProcessor(
  private val myService: Service,
) : IndexerProcessor {
    override fun process(events: List<IndexedEvent>, block: Block?) {
        myService.processEvents(events)
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

## Java compatibility

When using the indexer-core package in a Java project, prefer using `startInCoroutine()` to start your indexer implementation,
as the regular `start()` is a suspend function for which the caller has to supply a coroutine scope.
Otherwise, the package is Java compatible as is.
