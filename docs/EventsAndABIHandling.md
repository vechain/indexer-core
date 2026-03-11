# Events and ABI Handling

Event decoding in `indexer-core` is driven by `CombinedEventProcessor`, which is configured for you through `IndexerFactory`.

The current event stack is:

- `AbiLoader`: loads ABI JSON resources from the classpath
- `AbiEventProcessor`: decodes ABI events and optional VET transfers
- `BusinessEventProcessor`: derives higher-level business events
- `CombinedEventProcessor`: merges ABI events and business events into one output stream

## Recommended Usage

In most applications you should configure event decoding through `IndexerFactory` rather than constructing processors manually.

```kotlin
val indexer =
    IndexerFactory()
        .name("events")
        .thorClient(thorClient)
        .processor(processor)
        .abis("abis")
        .abiEventNames(listOf("Transfer", "Approval"))
        .abiContracts(listOf("0xabc..."))
        .includeVetTransfers()
        .build()
```

That configuration builds a `CombinedEventProcessor` internally and emits decoded `IndexedEvent` values inside `IndexingResult`.

## ABI Resource Loading

ABI files are loaded from the classpath, not from arbitrary filesystem paths.

`AbiLoader` scans a base resource directory for `.json` files up to two levels deep.

```kotlin
val events = AbiLoader.loadEvents(
    basePath = "abis",
    eventNames = listOf("Transfer", "Approval"),
)
```

```kotlin
val functions = AbiLoader.loadFunctions(
    basePath = "abis",
    functionNames = listOf("balanceOf", "totalSupply"),
)
```

Behavior to be aware of:

- if `eventNames` or `functionNames` is empty, all matching ABI elements are loaded
- duplicate ABI elements are deduplicated by signature shape
- if a requested name is not present, loading fails with `IllegalArgumentException`
- placeholder substitution is supported with `${NAME}` syntax

### Placeholder Substitution

Both ABI and business-event JSON can contain placeholders such as `${TOKEN_NAME}`.

Values are resolved from:

1. the substitution map you pass in
2. environment variables

If any placeholders remain unresolved, loading fails.

## ABI Event Decoding

`AbiEventProcessor` can decode events from either:

- a full `Block`
- Thor `EventLog` and `TransferLog` payloads

For each decoded ABI event it produces an `IndexedEvent` containing:

- block ID and block number
- timestamp
- transaction ID
- origin
- gas metadata when block data is available
- raw topics/data when present
- decoded parameter values in `params`
- `eventType`
- `clauseIndex`

## Filtering Options

There are two layers of filtering.

### ABI-Level Filtering

Configured through the factory:

- `abiEventNames(...)`: load only selected ABI event names
- `abiContracts(...)`: accept events only from selected contract addresses

### Thor Log Filtering

These filters are applied before decoding in `LogsIndexer` mode:

- `eventCriteriaSet(...)`
- `transferCriteriaSet(...)`

This is the most efficient way to reduce remote log volume.

## VET Transfers

Native VET transfers are not ABI events, but the library can represent them as synthetic `IndexedEvent` values with:

- `eventType = "VET_TRANSFER"`
- params:
  - `from`
  - `to`
  - `amount`

Enable them with:

```kotlin
IndexerFactory()
    .includeVetTransfers()
```

This works in both block-based and log-based processing.

It is also enabled automatically when a configured business event definition depends on `VET_TRANSFER`.

## Event Signatures and Matching

`EventUtils` provides the low-level helpers used by the runtime:

- `getEventSignature("Transfer(address,address,uint256)")`
- ABI/topic matching
- indexed and non-indexed parameter decoding

ABI matching is based on:

- topic0 signature
- indexed parameter count
- optional contract-address filtering

## Manual Usage

If you need direct access to the event stack outside `IndexerFactory`, you can construct a combined processor manually:

```kotlin
val processor =
    CombinedEventProcessor.create(
        abiBasePath = "abis",
        abiEventNames = listOf("Transfer"),
        abiContracts = listOf("0xabc..."),
        includeVetTransfers = true,
        businessEventPath = null,
        businessEventAbiBasePath = null,
        businessEventNames = emptyList(),
        businessEventContracts = emptyList(),
        substitutionParams = emptyMap(),
    )
```

Then process either a block:

```kotlin
val events = processor.processEvents(block)
```

or log payloads:

```kotlin
val events = processor.processEvents(eventLogs, transferLogs)
```

## Interaction with Business Events

When both ABI events and business events are enabled:

- business events are derived from decoded ABI events and optional VET transfers
- if a business event matches the same `txId` and `clauseIndex` as an ABI event, the ABI event is removed from the final output

This keeps the final event list focused on the higher-level semantic event where possible.

See [`BusinessEvents.md`](./BusinessEvents.md) for the business-event definition model.
