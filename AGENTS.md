# Agent Guide

This file is the canonical instruction entry point for coding agents working with `indexer-core`.

It is intentionally lean. Use it to build a correct mental model quickly, then open the linked repo docs instead of inferring behavior from scattered source files.

## What This Library Is

`indexer-core` is a Kotlin library for building VeChainThor indexers.

At a high level it provides:

- `IndexerProcessor` as the application persistence boundary
- `IndexerFactory` as the only supported way to configure and build indexers
- `IndexerRunner` to initialise, fast-sync when possible, coordinate dependencies, and keep indexers running through retries and reorg recovery
- two runtime modes:
  - `LogsIndexer` for fast log-based catch-up when you only need decoded events / transfers
  - `BlockIndexer` when you need full block context or dependency ordering

Do not ask users to construct indexers manually from implementation classes unless they are working on the library internals themselves. For normal usage, all indexers should be built with `IndexerFactory`.

## Who This Guide Is For

This guide is for both:

- agents changing `indexer-core` itself
- agents helping a consumer integrate `indexer-core` into another service

If the task is library maintenance, preserve public behavior documented in the repo docs unless the change explicitly updates that behavior.

If the task is consumer guidance, optimize for correct mode selection and integration advice before discussing internals.

## Required Onboarding Path

Before making claims about library behavior, read in this order:

1. [`README.md`](README.md)
2. [`docs/README.md`](docs/README.md)
3. one targeted guide based on the task:
   - runtime model and lifecycle: [`docs/IndexerOverview.md`](docs/IndexerOverview.md)
   - log-based mode and fast sync: [`docs/LogsIndexerOverview.md`](docs/LogsIndexerOverview.md)
   - ABI loading and decoded events: [`docs/EventsAndABIHandling.md`](docs/EventsAndABIHandling.md)
   - business event design: [`docs/BusinessEvents.md`](docs/BusinessEvents.md)
   - upgrade / compatibility questions: [`docs/MIGRATION-8.0.0.md`](docs/MIGRATION-8.0.0.md)

The repo markdown docs are the source of truth. Prefer them over memory, ad hoc code reading, or external copies.

## Mental Model To Keep In Mind

- `IndexerProcessor` is where consumers persist progress and domain data.
- The runtime may emit either `IndexingResult.LogResult` or `IndexingResult.BlockResult`; processors should handle both when relevant to the configuration.
- Startup rollback is intentional. It is a data-integrity feature, not a bug.
- Reorg recovery is part of the runtime contract. Consumers are expected to implement deterministic rollback behavior.
- Dependencies affect execution semantics, not just throughput. Adding `dependsOn(...)` changes how the runtime must coordinate indexers.

## Mode Selection Checklist

Use this checklist before recommending or editing indexer configuration.

Choose the default factory-built log mode when:

- the consumer only needs decoded ABI events, business events, or VET transfers
- fastest catch-up is the priority
- there is no same-block dependency on another indexer

Choose `includeFullBlock()` when the consumer needs:

- full block contents
- reverted transaction visibility
- gas / fee metadata from full block processing
- clause inspection results from `callDataClauses(...)`

Choose `dependsOn(...)` when:

- one indexer must finish a block before another processes that same block

Important:

- `LogsIndexer` and `BlockIndexer` are not interchangeable modes.
- `dependsOn(...)` forces block-based execution semantics.
- `includeFullBlock()` forces block-based execution semantics.

Choose business events when:

- downstream consumers care about higher-level actions rather than every raw event

Choose raw ABI events when:

- the consumer needs each decoded event individually
- there is no stable semantic grouping worth encoding as a business event

## Guardrails

- Build indexers through `IndexerFactory`, not by manually wiring implementation classes in application code.
- Do not describe `LogsIndexer` and `BlockIndexer` as equivalent choices with different performance profiles. They expose different runtime behavior and different data.
- Do not treat startup rollback as suspicious behavior. It is part of the library’s safety model.
- Do not rely on stale documentation copies. The repo docs are authoritative.
- Do not present internal implementation details as stable public API unless they are explicitly documented as such.

## Common Agent Tasks

Optimize guidance for these common tasks:

- explaining how to integrate `indexer-core` into another service
- changing the library itself
- debugging behavior differences between `LogsIndexer` and `BlockIndexer`
- designing ABI-driven or business-event-driven indexing setups

Documentation updates matter, but they are secondary to preserving correct runtime behavior and public guidance.

## Verification Expectations

When changing this library:

- run targeted tests for the touched behavior as a minimum
- run broader `./gradlew test` when the change is cross-cutting or affects shared runtime behavior
- run formatting checks or formatting fixes when Kotlin code changes

Minimum standard before claiming completion:

- the changed behavior is covered by tests or an existing test path was exercised
- any affected public guidance remains consistent with the repo docs
- the response states clearly if full verification was not run

Useful commands:

```bash
./gradlew test
./gradlew test --tests "org.vechain.indexer.SomeTest"
./gradlew spotlessCheck
./gradlew spotlessApply
```

## When Working From Source

The codebase is useful for confirmation, but agents should not need to reverse-engineer the library from source just to understand its purpose.

Read source after the docs when you need to:

- confirm an implementation detail
- debug a behavioral discrepancy
- update internals while preserving the documented contract

If source and docs appear to disagree, call that out explicitly instead of silently choosing one.
