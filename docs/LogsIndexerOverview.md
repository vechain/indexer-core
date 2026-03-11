# Logs Indexer Overview

`LogsIndexer` is the default indexer produced by `IndexerFactory`. It is designed for the common case where you want decoded events and VET transfers without processing every full block during catch-up.

## When `LogsIndexer` Is Used

`IndexerFactory.build()` returns a `LogsIndexer` when:

- `includeFullBlock()` is not enabled
- `dependsOn(...)` is not set

If either of those is set, the factory returns a `BlockIndexer` instead.

## What It Does

`LogsIndexer` fast-syncs by querying Thor log endpoints in block ranges:

- event logs from `/logs/event`
- transfer logs from `/logs/transfer`

It then decodes those logs into `IndexedEvent` values and emits:

```kotlin
IndexingResult.LogResult(
    endBlock = ...,
    events = ...,
    status = ...
)
```

## Fast Sync Behaviour

`LogsIndexer` implements `FastSyncableIndexer`.

Its fast-sync path is:

1. move to `FAST_SYNCING`
2. fetch the latest finalized block
3. process log batches until the current block reaches that finalized block number
4. move back to `INITIALISED`

This is different from the older behavior documented previously. The current implementation does not switch to a separate “near-tip block mode” based on a threshold. Steady-state block processing is coordinated by `IndexerRunner` after fast sync completes.

## Configuration

These options are exposed through `IndexerFactory` and apply to `LogsIndexer` mode:

- `abis(basePath)`
- `abiEventNames(eventNames)`
- `abiContracts(contractAddresses)`
- `businessEvents(basePath, abiBasePath)`
- `businessEventNames(eventNames)`
- `businessEventContracts(contractAddresses)`
- `businessEventSubstitutionParams(params)`
- `includeVetTransfers()` / `excludeVetTransfers()`
- `eventCriteriaSet(criteria)`
- `transferCriteriaSet(criteria)`
- `blockBatchSize(size)`
- `logFetchLimit(limit)`

### `blockBatchSize(...)`

Controls the size of the block range queried per log-sync batch.

Default: `100`

### `logFetchLimit(...)`

Controls the page size used for each Thor log request.

Default: `1000`

### `eventCriteriaSet(...)`

Narrows `/logs/event` requests before decoding.

```kotlin
val criteria =
    listOf(
        EventCriteria(
            address = "0xabc...",
            topic0 = "0xddf252ad..."
        )
    )
```

### `transferCriteriaSet(...)`

Narrows `/logs/transfer` requests before decoding.

```kotlin
val criteria =
    listOf(
        TransferCriteria(
            sender = "0x123...",
            recipient = "0x456...",
        )
    )
```

## Example

```kotlin
val tokenIndexer =
    IndexerFactory()
        .name("token-events")
        .thorClient(thorClient)
        .processor(processor)
        .startBlock(19_000_000)
        .abis("abis")
        .abiEventNames(listOf("Transfer", "Approval"))
        .abiContracts(listOf("0x0000000000000000000000000000456e65726779"))
        .includeVetTransfers()
        .eventCriteriaSet(
            listOf(
                EventCriteria(
                    topic0 = "0xddf252ad..."
                )
            )
        )
        .build()
```

## Processing Model

For each batch:

1. the batch end block is computed from `currentBlock + blockBatchSize - 1`
2. matching event logs are fetched if ABI processing is configured
3. matching transfer logs are fetched if VET transfers are enabled
4. decoded events are emitted via `IndexerProcessor.process(...)`
5. the current block is advanced to `batchEndBlock + 1`

If a batch contains no logs, the block pointer still advances.

## Returned Data

The processor receives decoded `IndexedEvent` items. Depending on configuration, those may include:

- ABI-decoded events
- synthetic `VET_TRANSFER` events
- business events derived from decoded ABI and transfer data

If both ABI events and business events are enabled, ABI events that are covered by a business event for the same `txId` and `clauseIndex` are removed from the final output.

## When Not to Use `LogsIndexer`

Use `BlockIndexer` instead when you need:

- access to the full `Block`
- reverted transactions
- gas and fee details from full block processing
- contract call inspection through `callDataClauses(...)`
- dependency ordering via `dependsOn(...)`
