# Storage startup manifest for fast secondary-index recovery

## Problem

Every time a `WritableStorage` instance opens, it must discover and restore all secondary
indexes that were registered in the previous session.  Without a manifest the only way to
do this is a full filesystem scan:

1. **`scanFiles()`** — an O(N) `readdir` over the data/index directory.
2. **Per-index file open** — for each `.index` file found, `openIndex()` opens the file,
   reads its binary header (magic + metadata block) to reconstruct the matcher, reads the
   file length to restore `index.data.length`, then closes the fd.

With 5 000 secondary indexes this means at least 5 000 file-open/read/close cycles at
startup, even before a single document is processed.  Startup time grows linearly with the
number of indexes.

## Solution: persist a manifest at graceful shutdown

`WritableStorage` writes a single JSON manifest file to the index directory on every
graceful `close()`.  On the next `open()`, if the manifest exists and its HMAC
verifies, the storage restores all secondary indexes and partition registrations from the
manifest **without any file I/O**, achieving nearly O(1) startup (bounded only by the time
to read and parse the manifest).

## Manifest format

```
.<storageName>.manifest.json
```

```json
{
  "version": 1,
  "partitions": ["bench.0", "bench.1", "bench.100"],
  "indexes": {
    "stream-orders": {
      "matcher": { "stream": "orders" },
      "length": 4712,
      "headerSize": 64
    },
    "audit-log": {
      "matcher": "function(doc) { return doc.audit === true; }",
      "length": 800,
      "headerSize": 64
    }
  },
  "hmac": "<sha256-hex of JSON.stringify({version, partitions, indexes})>"
}
```

Fields per index entry:

| Field        | Purpose |
|---|---|
| `matcher`    | Object matcher (stored as-is) or function matcher (serialised via `toString()`). |
| `length`     | Number of entries currently persisted to the index file. |
| `headerSize` | Byte offset of the first index entry in the file.  Allows reads to compute exact file positions without re-reading the binary header. |

Partition entries are the relative file names (relative to `dataDirectory`) used when each
partition was first registered, e.g. `"bench.0"`.

## Security model

A single HMAC (SHA-256, keyed with the existing `config.hmacSecret`) is computed over
the entire manifest body (`version + partitions + indexes`).  This covers:

- All matcher values, including serialised function matchers.
- All partition file names.
- The index lengths and header sizes.

**Why one document-level HMAC is sufficient**

When the manifest was written, all matchers were already trusted: they came from either
user-supplied code in the same process, or from individual index-file metadata that was
already HMAC-verified on a previous startup.  The manifest's document-level HMAC prevents
an attacker who can write to the disk from injecting a malicious function matcher.

If `hmacSecret` is the empty string (default), the HMAC is still computed and verified;
it just uses an empty key, which gives the same protection level as the existing per-matcher
HMAC approach with an empty secret.

## Startup decision tree (WritableStorage.open)

```
open(callback)
├── LOCK_RECLAIM mode?
│   └── yes → full scan + torn-write repair (manifest ignored)
└── no  → try loadManifest()
    ├── manifest missing or HMAC invalid?
    │   └── full scan (same as LOCK_RECLAIM, minus repair)
    └── manifest valid → restoreFromManifest()
        ├── register partitions (zero file I/O — creates objects, no open())
        ├── for each index: create WritableIndex via manifestData option
        │   (sets opened=true, data length, headerSize — no file open)
        ├── register matchers in IndexMatcher
        ├── emit 'index-created' for each index (EventStore picks up streams)
        ├── set initialized = true
        └── openIndexes() → open primary index, emit 'opened'
```

## Persistence timing and crash safety

The manifest is written **only on graceful `close()`**, after all write buffers have
been flushed to disk.  This makes it robust by design:

- **No crash → manifest valid**: next startup takes the fast path.
- **Crash before close**: manifest is missing or reflects the state of the *last* graceful
  shutdown.  Next startup (with `LOCK_RECLAIM`) falls back to the full scan and
  torn-write repair path, just as it did before this feature.  On the following graceful
  `close()`, a fresh manifest is written.
- **Manifest written then crash**: impossible — the process exits before or after
  `close()` completes, never in the middle of `close()` after manifest write but before
  other cleanup (the manifest write is the last step).

Because recovery is not performance-critical and a full scan is expected in that case,
there is no need for transactional or crash-safe manifest writes (e.g. write-then-rename).

## Performance expectation

| Startup path  | Complexity | Bottleneck |
|---|---|---|
| Manifest (happy path) | O(manifest file size) | JSON parse of ~70 bytes/index |
| Full scan fallback    | O(N indexes × file open cost) | per-index file open + header read |

For 5 000 indexes with simple object matchers the manifest file is roughly 300–400 KB.

Latest local re-run (Node v24.18.0, same benchmark harness copied into dedicated worktrees for `main`, `PR #339`, and this branch) with **non-empty** secondary indexes:

- **Scenario A (1 index per partition)** startup improves over `main` at higher counts (`5k: 279.8 → 236.0 ms`, `10k: 540.9 → 453.3 ms`, `20k: 971.7 → 902.5 ms`).
- **Scenario B (100 partitions, growing indexes)** startup is mixed (`100: 8.0 → 7.2 ms`, `1k: 28.4 → 40.8 ms`, `5k: 157.5 → 145.7 ms`).
