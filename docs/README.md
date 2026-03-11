# Documentation Index

This `docs/` directory is the canonical source of truth for detailed `indexer-core` documentation.

## Recommended Documentation Model

To keep documentation maintainable:

- keep [`README.md`](../README.md) short and focused on overview, installation, and quick start
- keep the detailed technical documentation in this directory
- update these markdown files in the same changeset as the code they describe
- use Confluence as a discovery and onboarding entry point that links back here

That keeps versioned technical documentation close to the code and avoids maintaining two divergent long-form doc sets.

## Guides

- [`IndexerOverview.md`](./IndexerOverview.md)
  - runtime primitives, lifecycle, status model, dependency ordering, and runner behavior
- [`LogsIndexerOverview.md`](./LogsIndexerOverview.md)
  - log-based indexing, fast sync, filtering, and when to use `LogsIndexer`
- [`EventsAndABIHandling.md`](./EventsAndABIHandling.md)
  - ABI resource loading, event decoding, filtering, VET transfers, and manual processor usage
- [`BusinessEvents.md`](./BusinessEvents.md)
  - business event JSON definitions, matching flow, rules, and interaction with ABI events
- [`MIGRATION-8.0.0.md`](./MIGRATION-8.0.0.md)
  - breaking changes when upgrading from 7.x

## Suggested Confluence Shape

If you keep a Confluence page for this library, it should ideally contain only:

- a short overview of what `indexer-core` is for
- a quick-start snippet
- links to the canonical repo docs in this directory
- any team-specific operational notes that do not belong in the library repo

Avoid copying the full technical guides into Confluence manually. That duplication will drift.
