# indexer-core 11.0.0 Migration Guide

## Breaking Changes

There is one breaking change in 11.0.0, affecting consumers that use `dependsOn(...)` and that resync a parent indexer out of band (for example by dropping its persisted state).

### Startup alignment no longer rolls children back to their parent's level

In 10.x and earlier, `IndexerRunner` aligned every dependency edge symmetrically at startup. If a child indexer was found persisted at a higher block than its parent, the runner would roll the child back to the parent's block so the chain could run in lockstep.

In 11.0.0, alignment is one-directional. Only **ancestors** are pulled back to a behind-descendant — a child that sits **above** its parent is left in place. The runtime's skip path waits for the parent to catch up, and same-block ordering resumes from the point at which the two meet.

### Why this changed

Operators resyncing a parent (for example to apply a logic change or backfill new fields) were forced to also pay the cost of rolling every dependant back to the parent's start block, even when the dependant did not actually need the parent's new data invalidated. The old behavior was correctness-first; the new behavior trades that safety net for cheaper resyncs and pushes the rollback decision to the operator.

### What you need to do

Audit each `dependsOn(...)` edge in your topology and decide what your dependant's `process(...)` actually reads from the parent.

**Same-block ordering only (no parent-state reads).** No action needed. The new behavior is correct for you — your dependant's persisted data is independent of the parent's contents at any given block.

**Reads parent persisted state in `process(...)`.** When you resync a parent, you must now also roll back any dependants that read the parent's state. Previously the library did this for you; in 11.0.0 it does not. The runner will log a WARN at startup naming every child that sits ahead of its parent so the condition is visible — confirm each warning matches an intentional resync, and roll dependants back manually if their stored data is now stale.

### Migration checklist

- [ ] Identify which dependants in your topology read parent persisted state inside `process(...)`.
- [ ] Update any internal resync runbook to include the dependants that previously aligned automatically.
- [ ] After upgrading, watch for `WARN ... is at block ... while its dependency ... is at block ...` log lines on first startup. Each line names a child that is ahead of its parent — confirm it is intentional and act on stale data if needed.

### What did not change

- Parent-ahead alignment is unchanged: if an ancestor is ahead of a behind-descendant, the ancestor still rolls back to the descendant's level (bounded by the ancestor's `startBlock`).
- Same-block ordering during forward progress is unchanged: `dependsOn(...)` still guarantees the parent finishes a given block before the child processes that same block.
- The `AlignmentFailure` aggregation behavior is unchanged: when an ancestor's processor cannot honour the requested rollback, every stuck indexer is surfaced in a single `IllegalStateException`.
- Fast-sync bypass for indexers with dependants is unchanged.
