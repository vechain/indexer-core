# Indexer Overview

## What is the Indexer?

The `Indexer` class is the core component of `indexer-core`. It is an abstract class that provides a framework for synchronizing blockchain data from the VeChainThor network. By extending this class, developers can create custom indexers tailored to their needs.

## Responsibilities of the Indexer

An indexer is responsible for:

1. Tracking the last synced block – Ensuring data integrity across runs.

2. Processing blockchain blocks – Extracting relevant data from transactions and events.

3. Handling re-organizations – Rolling back data if the blockchain forks.

4. Managing data storage – Storing and indexing extracted data.
----

## Implementing the Indexer

To use the indexer, create a new class that extends Indexer and implements the required methods.

#### Example

```
class MyCustomIndexer(thorClient: ThorClient) : Indexer(thorClient) {
    override fun getLastSyncedBlock(): BlockIdentifier? {
        // Fetch last processed block from storage
        return null
    }

    override fun rollback(blockNumber: Long) {
        // Implement rollback logic
    }

    override fun processBlock(block: Block) {
        // Handle block processing
    }
}
```

### Explanation

- `getLastSyncedBlock()`: Retrieves the last successfully processed block.

- `rollback(blockNumber: Long)`: Reverts any changes made by a specific block.

- `processBlock(block: Block)`: Extracts and processes relevant data from the block.
---


## How the Indexer Works

### Initialization

When the indexer starts, it:

1. Calls `getLastSyncedBlock()` to determine where to resume indexing.

2. Calls `rollback()` to revert the last processed block (if needed for data consistency).

3. Begins processing new blocks from the last synced position.

### Processing Blocks

Each block contains transactions and events. The `processBlock()` method should:

1. Extract relevant transactions and events.

2. Store extracted data in a database.

3. Update the last processed block number.

### Handling Re-orgs

Blockchain reorganizations occur when:

2. A different chain becomes the canonical chain.

3. Previously indexed blocks are no longer valid.

The rollback() method is used to remove or correct data from orphaned blocks.

----

## Running the Indexer

To start the indexer, call:
````
val myIndexer = MyCustomIndexer(thorClient)
myIndexer.startInCoroutine()
````

This runs the indexer inside a coroutine, ensuring non-blocking execution.

For Java compatibility, use:

```
myIndexer.startInCoroutine();
```

## Best Practices

- Ensure rollback logic is implemented to prevent data inconsistencies.

- Store the last synced block persistently to avoid redundant processing.

- Run the indexer as a background service to ensure continuous synchronization.
---
## Summary

The `Indexer` class is the foundation for building custom indexers. By implementing `getLastSyncedBlock()`, `rollback()`, and `processBlock()`, developers can create scalable, efficient indexers that extract valuable insights from VeChainThor.