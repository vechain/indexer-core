# Logs Indexer Documentation

## Introduction

The `LogsIndexer` is an optimized indexing mechanism designed to **process blockchain logs directly** instead of iterating through full blocks. This approach significantly improves **indexing speed and efficiency**, making it ideal for **real-time event tracking**.

Unlike `BlockIndexer`, which processes **entire blocks**, `LogsIndexer` fetches **only relevant logs (event logs & transfer logs)** from the VeChainThor blockchain, reducing the amount of unnecessary data processing.

---

## Responsibilities of the Logs Indexer

A `LogsIndexer` is responsible for:

1. **Tracking the last synced block** – Ensuring log consistency across runs.
2. **Fetching logs from VeChainThor** – Using event and transfer log queries instead of full block processing.
3. **Filtering and decoding logs** – Extracting relevant logs based on criteria.
4. **Handling re-organizations** – Rolling back processed logs if the blockchain forks.
5. **Managing data storage** – Storing and indexing extracted logs efficiently.

---

## How Logs Indexer Works

### **1️⃣ Fast Sync Mode (Log-Based Indexing)**
- Fetches **event logs** and **VET transfer logs** using:
  - `getEventLogs()` for smart contract events.
  - `getVetTransfers()` for VET transfers.
- Uses **batch processing** with `BLOCK_BATCH_SIZE` to **retrieve multiple blocks' logs at once**.
- Stores only the relevant **decoded events** and **transfers**.
- Skips unnecessary transactions, unlike block-based processing.

### **2️⃣ Switching to Block Indexing**
- The indexer operates in **Fast Sync Mode** until it reaches `blockSwitchThreshold`.
- Once within the threshold (e.g., **1000 blocks behind the best block**), it **switches to `BlockIndexer`**.
- The reason for switching:
  - **Better finality**: Ensures blockchain reorgs don’t lead to missing logs.

---

## Implementing the Logs Indexer

To use the `LogsIndexer`, create a new class that extends it and implements `processLogs()`.

#### Example:

```kotlin
class MyLogsIndexer(
    thorClient: ThorClient,
    abiManager: AbiManager,
    businessEventManager: BusinessEventManager
) : LogsIndexer(thorClient, abiManager, businessEventManager) {
    override fun processLogs(events: List<EventLog>, transfers: List<TransferLog>) {
        events.forEach { event ->
            println("Processed event: ${event.topics[0]} at block ${event.meta.blockNumber}")
        }
        transfers.forEach { transfer ->
            println("Processed VET transfer from ${transfer.sender} to ${transfer.recipient}")
        }
    }
}
```

---

## Logs Indexer Lifecycle

### **1️⃣ Initialization**
When the indexer starts, it:

1. Determines where to resume indexing using the last synced block.
2. Calls `rollback()` to revert logs from the last block (if needed for consistency).
3. Begins processing logs in batches.

### **2️⃣ Processing Logs**
The `processLogs()` method is where users implement their logic to handle **only the relevant logs** provided as input. The logs that are passed to this method have already been filtered according to configured `eventCriteriaSet` and `transferCriteriaSet`. The method should:

1. **Store logs** – Save logs to a database, cache, or external system.
2. **Trigger business logic** – Update indexes, trigger alerts, or notify other systems.
3. **Ensure data integrity** – Handle duplicates and missing data gracefully.

#### Example:
```kotlin
override fun processLogs(events: List<EventLog>, transfers: List<TransferLog>) {
    events.forEach { event ->
        logger.info("Storing event: ${event.topics[0]} from block ${event.meta.blockNumber}")
    }
    transfers.forEach { transfer ->
        logger.info("Recording VET transfer from ${transfer.sender} to ${transfer.recipient}")
    }
  
  // Optionally decode events with processAllEvents(events: List<EventLog>, transfers: List<TransferLog>)
}
```

### **3️⃣ Handling Re-orgs**
Blockchain reorganizations occur when:
- A different chain becomes the canonical chain.
- Previously indexed logs are no longer valid.

The `rollback()` method is used to remove logs from orphaned blocks.

---

## Logs Indexer Configuration

The `LogsIndexer` class allows configuration parameters to be set **at initialization**. These values define how logs are fetched, processed, and indexed.

#### **Where These Are Set**

These parameters are passed to the `LogsIndexer` constructor:

```kotlin
abstract class LogsIndexer(
    override val thorClient: ThorClient,
    startBlock: Long = 0L,
    private val syncLoggerInterval: Long = 1_000L,
    private val blockSwitchThreshold: Long = 1_000L,
    private val logsType: Set<LogType> = setOf(LogType.EVENT),
    private val blockBatchSize: Long = 100L,
    private val logFetchLimit: Long = 1_000L,
    private var eventCriteriaSet: List<EventCriteria>? = null,
    private var transferCriteriaSet: List<TransferCriteria>? = null,
    final override val abiManager: AbiManager? = null,
    override val businessEventManager: BusinessEventManager? = null,
) : BlockIndexer(thorClient, startBlock, syncLoggerInterval, abiManager, businessEventManager)
```

### **🛠 Configuration Parameters**

| Parameter                | Default Value | Description                                                 |
|--------------------------|--------------|-------------------------------------------------------------|
| `syncLoggerInterval`     | `1000`      | Defines how often log sync progress is printed.             |
| `blockSwitchThreshold`   | `1000`      | The number of blocks before switching to `BlockIndexer`.    |
| `logsType`               | `EVENT` | Determines which logs are indexed (`EVENT`, `TRANSFER`, or both). |
| `blockBatchSize`         | `100`       | The number of blocks processed in a single batch.           |
| `logFetchLimit`          | `1000`      | The maximum number of logs retrieved in one request.        |
| `eventCriteriaSet`       | `null`      | Optional filtering criteria for **event logs**.             |
| `transferCriteriaSet`    | `null`      | Optional filtering criteria for **transfer logs**.          |

---

## Summary
- `LogsIndexer` fetches logs **instead of full blocks**, making it much faster.
- Uses **batch processing** to improve efficiency.
- Switches to `BlockIndexer` **near latest blocks** to ensure finality.
- Supports **both event logs and VET transfers**.
- Can be **customized** via constructor parameters.
- Handles **chain reorganizations with rollback support**.

By using `LogsIndexer`, you can **speed up blockchain indexing** without sacrificing data integrity! 🚀

