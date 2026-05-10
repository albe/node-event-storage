# Changelog

## 1.1.0 (to be released)

Minor breaking change: the low-level `Storage` API now completes opening asynchronously via `open()` / `'opened'`. This does not affect the top-level `EventStore` API layer, which still becomes ready through its existing `'ready'` event.

New in 1.1:

- hierarchical slash-separated stream names, alongside the existing dash-based category naming
- richer query-based commit conditions for dynamic consistency boundary workflows
- additional storage tuning options for large numbers of streams and partitions

## 1.0.0

Baseline 1.0 feature set:

- streams
- commit with expected version
- read-only mode
- consumers
- ESM syntax
