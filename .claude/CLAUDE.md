# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Kotlin library for building blockchain indexers for VeChainThor. It provides parallel processing, dependency management, and automatic retry logic for indexing blockchain data.

## Build Commands

```bash
# Build the project
./gradlew build

# Run tests
./gradlew test

# Run a specific test class
./gradlew test --tests "org.vechain.indexer.utils.IndexerOrderUtilsTest"

# Check code coverage
./gradlew jacocoTestReport
# Coverage report: build/reports/jacoco/test/html/index.html

# Format code (ktfmt with Google style)
./gradlew spotlessApply

# Check formatting
./gradlew spotlessCheck

# Clean build artifacts
./gradlew clean
```

## Core Architecture

### Indexer Types

The library provides two main indexer implementations via `IndexerFactory.build()`:

1. **BlockIndexer**: Full block-by-block processing
   - Used when `includeFullBlock()` is set or when `dependsOn()` is configured
   - Can inspect transaction call data via `callDataClauses()`
   - Processes reverted transactions
   - Required for dependent indexers

2. **LogsIndexer**: Fast event-based syncing
   - Used by default when no dependencies and full block not required
   - Fetches only event logs and transfer logs via Thor API
   - More efficient for event-driven indexing
   - Implements `fastSync()` to quickly catch up to finalized blocks

### Dependency Management & Sequential Processing

The `IndexerRunner` orchestrates multiple indexers using topological sorting (`IndexerOrderUtils.topologicalOrder()`):

- All indexers placed in single group, ordered by dependencies (dependencies before dependents)
- Indexers process same block **sequentially** within group, honoring dependency order
- IndexerRunner uses channels to buffer blocks and coordinate processing
- Example: If `IndexerA` depends on `IndexerB`, then `IndexerB` processes block N before `IndexerA` processes block N
- Only single-dependency chains supported (each indexer can depend on at most one other)

### Lifecycle & States

Indexer states (defined in `Status` enum):
- `NOT_INITIALISED` → `INITIALISED` → `FAST_SYNCING` → `SYNCING` → `FULLY_SYNCED`
- `PRUNING`: Temporary state when pruner runs (every `prunerInterval` blocks)
- `SHUT_DOWN`: Terminal state

Initialization flow:
1. `initialise()`: Determines starting block, calls `rollback()` on processor
2. `fastSync()`: (LogsIndexer only) Catches up to finalized block using log events
3. `processBlock()`: Main processing loop with reorg detection

### Reorg Detection

Reorg detection in `BlockIndexer.checkForReorg()`:
- Compares `block.parentID` with `previousBlock.id`
- On detection: logs error, calls `rollback()`, throws `ReorgException`
- Only checks when `currentBlockNumber > startBlock` and `previousBlock != null`

### Event Processing

Event processing pipeline (`CombinedEventProcessor`):
1. **ABI Events**: Configured via `abis()` - loads JSON ABI files
2. **Business Events**: Configured via `businessEvents()` - custom event definitions with conditional logic
3. **VET Transfers**: Included by default unless `excludeVetTransfers()` is called

Events are decoded and returned as `IndexedEvent` objects to the `IndexerProcessor.process()` method.

### IndexerProcessor Interface

Implementations must provide:
- `getLastSyncedBlock()`: Returns last successfully processed block (or null)
- `rollback(blockNumber)`: Reverts data for specified block
- `process(entry)`: Handles `IndexingResult.Normal` (full block) or `IndexingResult.EventsOnly` (log batch)

## Code Style

- **Formatting**: ktfmt with Google style, 4-space indents (enforced by Spotless)
- **Language**: Kotlin with Java 21 target
- **Testing**: JUnit 5, MockK for mocking, Strikt for assertions

## Important Implementation Details

### IndexerFactory Configuration

The factory uses a builder pattern. Key methods:
- `name()`, `thorClient()`, `processor()`: Required
- `startBlock()`: Default is 0
- `dependsOn()`: Forces BlockIndexer (needed for dependency coordination). Single-parent only.
- `includeFullBlock()`: Forces BlockIndexer (enables access to gas, reverted txs)
- `pruner()` + `prunerInterval()`: Optional periodic cleanup
- `blockBatchSize()`: For LogsIndexer, controls log fetch batch size (default 100). For IndexerRunner, controls channel buffer (default 1).
- `logFetchLimit()`: Pagination limit for Thor API calls (default 1000)

### Retry Logic

`IndexerRunner.retryUntilSuccess()` wraps:
- Indexer initialization
- Block fetching
- Block processing

On failure: logs error, waits 1 second, retries indefinitely (until success or cancellation).

### Pruner Execution

Pruners run when:
- `status == FULLY_SYNCED`
- `currentBlockNumber % prunerInterval == prunerIntervalOffset`
- Random offset prevents all indexers from pruning simultaneously

During pruning, status becomes `PRUNING` and processing pauses.

## Testing Notes

- Mock Thor client for unit tests
- Use `TestableLogsIndexer` pattern to test internal sync logic
- Verify topological ordering for dependency chains in `IndexerOrderUtilsTest`
- Test reorg scenarios by providing blocks with mismatched `parentID`

## Preferences
Be extremely concise. Sacrifice grammar for the sake of concision.
Always prefer simple solution over complex ones.
When unsure, ask for clarification.