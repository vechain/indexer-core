# Metrics

`IndexerRunner` records [Micrometer](https://micrometer.io) meters when it is given a registry:

```kotlin
IndexerRunner.launch(
    scope = scope,
    thorClient = thorClient,
    indexers = indexers,
    meterRegistry = meterRegistry,
)
```

Without one it records nothing. Micrometer is a facade, so the backend is the application's
choice; in a Spring Boot service, pass the auto-configured `MeterRegistry` and the meters appear on
`/actuator/prometheus` alongside the application's own.

Every meter is registered once, when `run` starts, for the indexers the runner was given. Names
below are Micrometer's; the Prometheus registry renders `indexer.current.block` as
`indexer_current_block`, adds `_total` to counters and `_seconds` to timers, and renders the
`indexer.name` tag as `indexer_name`.

## Per indexer

Tagged `indexer.name`.

| Meter | Type | Value |
| --- | --- | --- |
| `indexer.current.block` | gauge | The indexer's current block; `NaN` until it is initialised |
| `indexer.sync.status` | gauge | One-hot over `status` — one series per [`Status`](../src/main/kotlin/org.vechain.indexer/Indexer.kt) value, `1` for the current one |
| `indexer.blocks.processed` | counter | Blocks the indexer has advanced through while `SYNCING`, `FAST_SYNCING` or `FULLY_SYNCED` |
| `indexer.block.process` | timer | One successful `processBlock` call by the runner |
| `indexer.block.process.failures` | counter | `processBlock` attempts that threw, each of which the runner retries |

`indexer.blocks.processed` is computed when the meter is read, from forward movement of the
current block since the previous read. That is what lets a `LogsIndexer`'s fast sync count as it
advances, since the runner does not see the individual ranges. A rollback is not subtracted, and a
jump made outside the processing statuses — the cursor restored on startup — is not counted.

`indexer.block.process` covers the block path only; a fast sync's progress shows in
`indexer.current.block` and `indexer.blocks.processed`.

## Runner

| Meter | Type | Tags | Value |
| --- | --- | --- | --- |
| `indexer.runner.phase` | gauge | `phase` = `catch_up`, `steady` | One-hot; both `0` before the first classification |
| `indexer.runner.catch.up.group` | gauge | `indexer.name`, `group` = `1`, `2`, `3` | One-hot during catch-up: `1` fast-sync pending, `2` ready to sync, `3` blocked behind a fast-syncing ancestor. All `0` once steady |
| `indexer.runner.proximity.group` | gauge | `indexer.name`, `group` | One-hot over the [proximity groups](./IndexerOverview.md#proximity-grouping), `1` being the furthest behind. All `0` while the indexer is not in one, e.g. during its fast sync. A `group` series appears the first time the indexer joins that group |
| `indexer.runner.proximity.groups` | gauge | | How many proximity groups the runner last formed |
| `indexer.runner.regroups` | counter | `kind` = `catch_up`, `proximity` | Catch-up passes started, and proximity groupings formed |
| `indexer.runner.restarts` | counter | `reason` = `reorg`, `stuck_block` | Times the run loop restarted every indexer |

Group numbers are positions, not identities: after a regroup, group `2` may hold different
indexers. Read them as "how far behind is the group this indexer is running in". The group is a
tag rather than a value so that queries can join on it, e.g. the lowest `indexer.current.block`
among the members of each proximity group is that group's stream position.

The catch-up group is set when a pass starts and holds until the next one. An indexer in group `1`
whose `indexer.sync.status` is already `READY_TO_SYNC` has finished its fast sync and joins the
others when the pass ends.
