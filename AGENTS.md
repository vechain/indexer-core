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
   - upgrade / compatibility questions: [`docs/MIGRATION-8.0.0.md`](docs/MIGRATION-8.0.0.md), [`docs/MIGRATION-11.0.0.md`](docs/MIGRATION-11.0.0.md)

The repo markdown docs are the source of truth. Prefer them over memory, ad hoc code reading, or external copies.

## Mental Model To Keep In Mind

- `IndexerProcessor` is where consumers persist progress and domain data.
- The runtime may emit either `IndexingResult.LogResult` or `IndexingResult.BlockResult`; processors should handle both when relevant to the configuration.
- Startup rollback is intentional. It is a data-integrity feature, not a bug — but as of 11.0.0 it is one-directional: ancestors are aligned down to a behind-descendant, a child ahead of its parent is left in place. See `docs/MIGRATION-11.0.0.md`.
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

## Commit & Pull Request Guidelines

Follow the existing history: concise, imperative titles with a conventional-commit prefix (e.g.
`fix: narrow the block range when a batch exceeds Thor's log offset cap`). Describe problem,
solution, and verification in the PR body. Run formatters and tests locally before requesting
review.

### Prose Bloat Is a Merge Blocker

[.github/workflows/pr-bloat.yml](.github/workflows/pr-bloat.yml) fails a PR on a description over
240 words (or under 10), a tool-attribution trailer, or a comment block that outweighs the code it
documents (>1:1 against attached added-code lines; 10 lines absolute; a top-of-file header measures
against the whole file). Comment density and long markdown paragraphs are advisory; `**/*.md` never
blocks. `make check-pr-bloat` runs it locally. Bypass is the `verbose-ok` label, which anyone
including the author may apply. Thresholds are env-overridable in
[check_pr_bloat.py](.github/workflows/scripts/check_pr_bloat.py).

Write reviewer-facing prose at final length — don't draft long and trim. The budget is the target,
not a limit to approach.

- **PR descriptions:** what changed and why, in two or three sentences. Skip `## Summary` /
  `## Test plan` scaffolding unless there is genuinely something new to test. No tool-attribution
  trailers.
- **Comments:** add one only where the WHY is non-obvious — a hidden constraint, an invariant, a
  workaround. A comment must not outweigh the code it documents; on a one-line field addition, that
  means no comment. KDoc counts.
- Don't restate what the code does, or what a technical term already implies (a reader who knows
  `reorg` doesn't need "when the chain changes").
- Don't explain what something does _not_ do. State what is; the reader can see the absence.
- Long-form rationale belongs in [`docs/`](docs/), which this guide already treats as the source of
  truth, not stacked above the code.

## When Working From Source

The codebase is useful for confirmation, but agents should not need to reverse-engineer the library from source just to understand its purpose.

Read source after the docs when you need to:

- confirm an implementation detail
- debug a behavioral discrepancy
- update internals while preserving the documented contract

If source and docs appear to disagree, call that out explicitly instead of silently choosing one.
