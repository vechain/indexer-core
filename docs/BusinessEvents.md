# Business Events Handling

## Introduction

Business Events allow for high-level event detection by correlating multiple blockchain events within the same transaction. Instead of relying solely on individual event logs, Business Events define rules and conditions to derive meaningful insights, such as token swaps, rewards distribution, or specific contract interactions.

The `BusinessEventProcessor` works with the `BusinessEventManager` to:

1. Extract relevant events based on predefined business event definitions.

2. Apply conditions to filter and match events.

3. Validate rules that determine when a business event has occurred.

4. Map parameters from raw events into structured business event data.

To use this feature, you must configure an instance of `BusinessEventManager` before using `BusinessEventProcessor`. This ensures that all business event definitions are properly loaded and available for event processing.

---

## Setting Up Business Event Manager

The `BusinessEventManager` is responsible for loading and managing business event definitions from a specified directory.

### **Configuring  Business Event Manager Before Event Indexing**
Business Event Definition files must be **loaded as a mapping of file streams (`Map<String, InputStream>`)** before event processing.

#### Initializing Business Event Manager
```kotlin
val businessEventsFileStreams = loadAbiFiles("businessEvents") // Replace with your own implementation

val businessEventManager = BusinessEventManager()
businessEventManager.loadBusinessEvents(businessEventsFileStreams)
````
This loads all business event definitions from the specified directory into memory.

#### Retrieving a Business Event Definition

Once loaded, you can retrieve a specific business event definition:

```kotlin
val businessEvent = businessEventManager.getBusinessEventDefinition("Token_FTSwap")
````
----
## Processing Business Events Inside an Indexer

The `BusinessEventProcessor` works alongside `BusinessEventManager` to detect business events from blockchain transactions.

#### Example: Processing Business Events in an Indexer
```kotlin
class MyBusinessEventIndexer(thorClient: ThorClient, businessEventManager: BusinessEventManager) : Indexer(thorClient) {
private val eventProcessor = BusinessEventProcessor(businessEventManager)

    override fun processBlock(block: Block) {
        val events = processBlockGenericEvents(block)
        val businessEvents = eventProcessor.getOnlyBusinessEvents(events, listOf("Token_FTSwap", "B3TR_ActionReward"))
        businessEvents.forEach { (indexedEvent, parameters) ->
            println("Detected business event: ${indexedEvent.eventType} with params: ${parameters.params}")
        }
    }
}
````

This ensures that every block processed by the indexer will extract and handle relevant business events based on predefined rules.

----
## Processing Business Events Standalone

You can also use `BusinessEventProcessor` outside of an indexer if you only need to process business events independently.

#### Example: Standalone Business Event Processing
```kotlin
val genericEvents = fetchBlockchainEvents()
val businessEvents = businessEventProcessor.getOnlyBusinessEvents(genericEvents, listOf("Token_FTSwap"))

businessEvents.forEach { (indexedEvent, parameters) ->
    println("Detected business event: ${indexedEvent.eventType} with params: ${parameters.params}")
}
````

## Business Event Processing Methods

#### `processAllEvents`

The processAllEvents method processes an entire blockchain block, evaluating every generic event in the transaction or clause. It then applies business event rules and returns a list of business events. If no valid business event is found, it returns the original generic events.

**Usage with an Indexer**
```kotlin
val businessEvents = eventProcessor.processAllEvents(block)
businessEvents.forEach { (indexedEvent, parameters) ->
    println("Detected business event: ${indexedEvent.eventType} with params: ${parameters.params}")
}
````
#### `getOnlyBusinessEvents`

This method only returns business events, filtering out generic events that do not match any business rules. It requires a list of generic events as input, making it ideal for use with `GenericEventIndexer`.

**Usage in an Indexer**
```kotlin
val genericEvents = processBlockGenericEvents(block)
val businessEvents = eventProcessor.getOnlyBusinessEvents(genericEvents, listOf("Token_FTSwap", "B3TR_ActionReward"))
businessEvents.forEach { (indexedEvent, parameters) ->
    println("Detected business event: ${indexedEvent.eventType} with params: ${parameters.params}")
}
````
---
## Filtering Options

Both `processAllEvents` and `getOnlyBusinessEvents` support filtering by business event names to limit processing to specific event types.

#### Example:
```kotlin
val businessEvents = eventProcessor.getOnlyBusinessEvents(genericEvents, listOf("Token_FTSwap"))
````
This ensures that only business events of type Token_FTSwap are processed.

----

## Understanding Business Event Fields

### Event Conditions

Event conditions define logical comparisons between different attributes of blockchain events. These conditions determine whether a specific event should be included as part of a business event.

Each condition consists of:

- **firstOperand**: The field from the event being compared.

- **isFirstStatic**: Whether `firstOperand` is a static value.

- **secondOperand**: The field or static value being compared against.

- **isSecondStatic**: Whether `secondOperand` is a static value.

- **operator**: The comparison operator (e.g., `EQ`, `NE`, `GT`, `LT`).

#### Example:
```json
{
    "firstOperand": "from",
    "isFirstStatic": false,
    "secondOperand": "origin",
    "isSecondStatic": false,
    "operator": "EQ"
}
````

This condition ensures that the from address of the event matches the transaction origin address.

### Rules

Rules define relationships between multiple events to ensure a valid business event occurs. Each rule consists of:

- `firstEventName`: The alias of the first event being compared.

- `firstEventProperty`: The specific field of `firstEventName` being checked.

- `secondEventName`: The alias of the second event.

- `secondEventProperty`: The field from `secondEventName` being compared.

- `operator`: The comparison operator (`EQ`, `NE`, `GT`, `LT`).

#### Example:
```json
{
    "firstEventName": "inputTransfer",
    "firstEventProperty": "from",
    "secondEventName": "outputTransfer",
    "secondEventProperty": "to",
    "operator": "EQ"
}
````
This rule ensures that the sender of the inputTransfer event is the same as the recipient of the outputTransfer event.


### `sameClause` Field

The `sameClause` field determines whether all events must come from the same transaction clause.

- `true`: Events must be part of the same clause.

- `false`: Events can be from different clauses within the same transaction. (default)

Use true when all events are expected to be tightly linked within a single execution, such as swaps.

---

### `checkAllCombinations` and When to Use It

By default, business event processing is selective. However, `checkAllCombinations` can be used to check every generic event in the transaction or clause and evaluate all possible valid combinations against the defined rules.

#### When Should You Use It?

- **Complex Event Detection**: When many events match the conditions, but only a subset will satisfy all rules.

- **Ensuring No Matches Are Missed**: It runs an exhaustive search to evaluate all possible valid event mappings, ensuring that no valid business event is overlooked.

- **Multiple Combinations**: If there are multiple ways to form a valid event set, this approach ensures all combinations are evaluated.

#### Why Not Use It by Default?

- **Performance overhead**: Evaluating all combinations can slow down processing.

- **Unnecessary computation**: Well-written conditions should capture expected events without exhaustive searching.

- **Increased resource usage**: More memory and processing power required.

If conditions are well-structured, default settings should be adequate.

----

##  Supported Operators
| Operator           | Description  |
|--------------------|--------------|
| `EQ`             | Equal to     |
| `NE`          | Not equal to |
| `GT`          | Greater than  |
| `LT`          | Less than     |
| `GE`          | Greater than or equal to |
| `LE`          | Less than or equal to |

----
## Summary

- `BusinessEventManager` must be configured before using `BusinessEventProcessor`.

- `processAllEvents**` processes all events in a block and returns business events, or generic events if none match.

- `getOnlyBusinessEvents` extracts only business events from a list of generic events, requiring events to be passed in.

- **Filters** can be applied to limit processing by business event names.

- **Operators** allow for precise rule enforcement when defining event conditions.

- Business event detection can be used inside an indexer or as a **standalone** utility.

By implementing Business Events, you can extract meaningful insights from raw blockchain data!