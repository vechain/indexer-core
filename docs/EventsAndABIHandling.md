# Events and ABI Handling

## Introduction

The `indexer-core` library provides tools for handling blockchain events. This includes **Generic Events** (decoded from smart contract logs) and **Business Events** (derived from correlated raw events). To process these, we utilize **ABI Management** and various filtering options.

This feature is optional. The `GenericEventIndexer` can be used to decode events, but it is not required to run the indexer. If your use case does not involve event processing, you can safely ignore this functionality.

However, to use this feature, you must configure an instance of `AbiManager` before using `GenericEventIndexer` to ensure ABI definitions are available.

If you are indexing VeChain contracts, you can find a collection of ABIs in the [b32 repository](https://github.com/vechain/b32), which contains all major VeChain smart contract ABIs.

---
## Generic Event Handling

Generic events are extracted directly from smart contracts using ABIs. The `GenericEventIndexer` simplifies this process by decoding raw event logs into structured data. If your class extends Indexer, you can call `processBlockGenericEvents` to extract and process events as part of your block processing logic.

#### Example: Extracting Events from a Block
```kotlin
// Create a mapping of ABI file streams in your own code
val abiFileStreams = loadAbiFiles("src/main/resources/abis")

// Configure ABI Manager
val abiManager = AbiManager()
abiManager.loadAbis(abiFileStreams)

// Implement GenericEventIndexer and extract events
val eventIndexer = GenericEventIndexer(abiManager)
val events = eventIndexer.getEvents(block)
````
Each extracted event contains:

- `IndexedEvent` – Metadata such as block number, transaction ID, and event signature.

- `GenericEventParameters` – Decoded event parameters and name.

### Processing Events Inside an Indexer

If your indexer class extends Indexer, you can directly use `processBlockGenericEvents` to extract and process events while handling a block. This method simplifies event extraction and ensures all relevant contract logs are processed efficiently.

#### Example: Processing Events in an Indexer
```kotlin
class MyCustomIndexer(thorClient: ThorClient, abiManager: AbiManager) : Indexer(thorClient) {
    private val eventIndexer = GenericEventIndexer(abiManager)

    override fun processBlock(block: Block) {
        val events = processBlockGenericEvents(block)
        events.forEach { (indexedEvent, parameters) ->
            println("Processed event: ${indexedEvent.eventType} with params: ${parameters.params}")
        }
    }
}
````
This ensures that every block processed by the indexer will automatically extract and handle relevant events.

#### Why Use `processBlockGenericEvents`?

- **Automatically extracts and decodes all contract events from a block.**

- **Reduces boilerplate code**, since event extraction is built into the indexer lifecycle.

- **Improves maintainability**, making it easier to extend and add new event processing logic later.

#### Filtering Events

To filter events, use:
```kotlin
val filteredEvents = eventIndexer.getEventsByFilters(
    block,
    abiNames = listOf("MyContract"),
    eventNames = listOf("Transfer"),
    contractAddresses = listOf("0x1234..."),
    vetTransfers = true
)
```
Filtering options:

- **abiNames**: Limits event extraction to specific contract ABIs.

- **eventNames**: Extracts only specific event types.

- **contractAddresses**: Filters events based on contract addresses.

- **vetTransfers**: Includes native VET transfer events.

- **removeDuplicates**: Ensures that duplicate events are not processed twice.

----
## Setting Up ABI Manager

The `AbiManager` is responsible for **loading and storing ABI definitions**. To use ABI-based event processing, it must be **initialized before** `GenericEventIndexer` to ensure ABI definitions are available when events are processed.

### **Configuring ABI Manager Before Event Indexing**
ABI files must be **loaded as a mapping of file streams (`Map<String, InputStream>`)** before event processing.

#### **Loading ABI Files**
```kotlin
val abiFileStreams = loadAbiFiles("abis") // Replace with your own implementation

val abiManager = AbiManager()
abiManager.loadAbis(abiFileStreams) // Load ABIs before processing events
````

#### Retrieving ABI Events
`````kotlin
val events = abiManager.getEventsByNames(listOf("MyContract"), listOf("Transfer"))
`````
This ensures that only known and defined events are decoded correctly.

#### ABI File (JSON Format)
```json
[
  {
    "type": "event",
    "name": "Transfer",
    "inputs": [
      { "name": "from", "type": "address", "indexed": true },
      { "name": "to", "type": "address", "indexed": true },
      { "name": "value", "type": "uint256", "indexed": false }
    ]
  }
]
````
----
## Event Decoding

The `EventUtils` class decodes event data using Solidity types. This can be used independently, but if you are working within an Indexer, the recommended approach is to use `processBlockGenericEvents`.
```kotlin
val decodedEvent = EventUtils.decodeEvent(event, abiElement)
````
It automatically:

1. Matches event topics with ABI signatures.

2. Extracts and converts indexed and non-indexed parameters.

3. Supports various Solidity data types (e.g., address, uint256, bytes).

#### Example: Computing an Event Signature
```kotlin
val signature = EventUtils.getEventSignature("Transfer(address,address,uint256)")
````
This returns the Keccak256 hash of the event signature.

----
## Summary

- Use `GenericEventIndexer` to extract and filter events from blocks.

- If extending Indexer, call `processBlockGenericEvents` inside `processBlock` to integrate event processing.

- `AbiManager` loads and retrieves ABI definitions for event decoding.

- `EventUtils` provides low-level utilities for decoding Solidity event data.

- This feature is **optional** and can be skipped if event indexing is not required.

- You must configure `AbiManager` before using GenericEventIndexer to ensure ABI definitions are available.
- VeChain ABIs are available in the [b32 repository](https://github.com/vechain/b32).

The next section will cover Business Events and how to correlate multiple raw events into meaningful actions.

