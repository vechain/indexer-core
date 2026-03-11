# Business Events

Business events let you derive higher-level actions from one or more decoded events in the same transaction.

Examples:

- token swaps
- staking flows
- reward claims
- composite actions spanning multiple clauses

In the current implementation, business events are configured from JSON resources and processed by `BusinessEventProcessor`.

## Recommended Usage

Configure business events through `IndexerFactory`:

```kotlin
val indexer =
    IndexerFactory()
        .name("stargate")
        .thorClient(thorClient)
        .processor(processor)
        .businessEvents("business-events", "abis")
        .businessEventNames(listOf("Stargate_Stake", "Stargate_Unstake"))
        .businessEventContracts(listOf("0xabc..."))
        .businessEventSubstitutionParams(
            mapOf(
                "STARGATE_NFT" to "0xabc...",
            )
        )
        .build()
```

This builds a `BusinessEventProcessor` internally and merges its output with any configured ABI events.

## How Business Events Are Built

Each business event definition declares:

- a `name`
- a list of source `events`
- optional `rules`
- output `paramsDefinition`
- optional matching controls such as `sameClause`, `checkAllCombinations`, and `maxAttempts`

Definitions are loaded from classpath JSON resources using `BusinessEventLoader`.

```kotlin
val definitions =
    BusinessEventLoader.loadBusinessEvents(
        basePath = "business-events",
        eventNames = listOf("MyBusinessEvent"),
        envParams = mapOf("TOKEN_ADDRESS" to "0xabc..."),
    )
```

Behavior to be aware of:

- if `eventNames` is empty, all definitions under the base path are loaded
- duplicate definitions are deduplicated by lowercase name
- if a requested definition name is missing, loading fails
- `${NAME}` placeholders are supported and resolved from the provided map first, then environment variables

## Processing Flow

At runtime:

1. the processor loads business event definitions
2. it derives the underlying ABI event names needed by those definitions
3. it decodes matching ABI events and any required VET transfers
4. events are grouped by block, then by transaction
5. each transaction is tested against the configured business-event definitions
6. when a match is found, a new `IndexedEvent` is emitted with `eventType = definition.name`

The emitted business event reuses metadata from the first matched source event:

- `blockId`
- `blockNumber`
- `blockTimestamp`
- `txId`
- `origin`
- `gasUsed`
- `gasPayer`
- `paid`
- `clauseIndex`

## Definition Structure

### Source Events

Each entry in `events` defines:

- `name`: ABI event name or `VET_TRANSFER`
- `alias`: local name used by rules and parameter mappings
- `conditions`: optional filters applied to that candidate event

Conditions compare either:

- event fields such as `origin`, `address`, `clauseIndex`, `signature`
- decoded parameter values from `params`
- static values

### Rules

Rules compare values across matched aliases.

Supported operators:

- `EQ`
- `NE`
- `GT`
- `LT`
- `GE`
- `LE`

Example rule:

```json
{
  "firstEventName": "inputTransfer",
  "firstEventProperty": "from",
  "secondEventName": "outputTransfer",
  "secondEventProperty": "to",
  "operator": "EQ"
}
```

### Output Parameters

`paramsDefinition` maps values from matched source events into the emitted business event.

The resulting business event is returned as an `IndexedEvent` whose `params.eventType` and `eventType` are both set to the business event name.

## Matching Controls

### `sameClause`

If `sameClause` is `true`, matching only occurs within a single clause.

If omitted or `false`, matching can span multiple clauses inside the same transaction.

### `checkAllCombinations`

By default, the processor maps aliases greedily.

If `checkAllCombinations` is `true`, it performs an exhaustive search for valid alias assignments and then validates the rules. This is more expensive and should be used only when simpler matching is insufficient.

### `maxAttempts`

When exhaustive search is enabled, `maxAttempts` limits the amount of backtracking work.

Default: `10`

## VET Transfer Support

Business-event definitions can reference `VET_TRANSFER` directly.

When they do, the runtime automatically enables transfer decoding for the supporting ABI processor even if you did not explicitly call `includeVetTransfers()`.

## Interaction with ABI Events

If you configure both ABI events and business events:

- ABI events are decoded first
- business events are derived from those decoded events
- ABI events that are covered by a business event with the same `txId` and `clauseIndex` are removed from the final output list

This prevents double-reporting of the same semantic action.

## Practical Guidance

Prefer business events when downstream consumers care about domain actions rather than raw logs.

Prefer plain ABI events when:

- you need every raw event individually
- there is no stable semantic grouping
- the matching rules would be overly complex or expensive
