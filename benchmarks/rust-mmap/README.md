# node-event-storage mmap benchmark (Rust)

A self-contained Rust program that replicates the **exact on-disk binary format** used by
[node-event-storage](https://github.com/albe/node-event-storage) and benchmarks every key
I/O operation using memory-mapped files.

## Purpose

The previous analysis showed that Node.js + mmap gives only a minimal speed improvement over
Node.js + `readSync` because `buffer.toString('utf8')` always copies bytes into a V8 string
heap regardless of the buffer's backing.

This benchmark answers: *"How much headroom exists if the same storage format is implemented
in a language where true zero-copy reads are possible?"*

## What it measures

| Benchmark | Description |
|---|---|
| write — Vec + bulk write | Build documents in a `Vec<u8>`, flush with one `write(2)` call |
| write — pre-allocated mmap | Pre-extend the file with `ftruncate`, then write directly into the mapped region |
| forward full scan | Iterate every document in order, parse each with `serde_json::from_slice` |
| forward scan (no JSON) | Same iteration, skip JSON parsing — measures pure mmap traversal overhead |
| backward full scan | Iterate in reverse using the O(1) footer-based step |
| range scan 1/3..2/3 | Scan the middle third of the partition |
| index point-lookup | Use the index file to jump to every 10th document |

## Key differences vs Node.js + mmap

### Zero-copy document parsing
In Node.js, every document read calls `buffer.toString('utf8', start, end)`, which **always**
allocates a new V8 string and copies `data_size` bytes regardless of whether the buffer is
memory-mapped or not.

In Rust, `serde_json::from_slice(&mmap[start..end])` parses **directly from the mapped region**.
String values that are only inspected (not retained) borrow from the slice and are never heap-
allocated. No copy occurs.

### O(1) backward traversal
The Node.js `findDocumentPositionBefore` method calls `buffer.lastIndexOf(DOCUMENT_SEPARATOR)`
when not at an exact boundary — a linear scan. Because the partition format stores the
`data_size` again just before the separator (the "footer"), the Rust backward iterator resolves
the previous document start in O(1):

```rust
let data_size = u32::from_be_bytes(mmap[abs - 8..abs - 4]);
let doc_start  = pos - document_write_size(data_size as usize);
```

### No GC pressure
V8's garbage collector must scan and collect the `Buffer` objects, `String` objects, and
intermediate parse results on every document read. Rust's stack-allocated reads and
deterministic drop require no GC pauses.

## How to run

```bash
# Default: 10 000 documents (matches bench-read-scenarios.js)
cargo run --release

# Custom document count
cargo run --release -- 100000
```

Requires Rust ≥ 1.70 (stable). No system libraries needed.

## File format compatibility

The files written by this benchmark are **byte-for-byte identical** to files written by
node-event-storage. After running the benchmark, the files in `/tmp/nes-bench-rust/` can
be opened with `ReadablePartition` and `ReadableIndex` from the Node.js library.

Relevant format constants:

| Field | Value |
|---|---|
| Partition magic | `nesprt03` |
| Index magic | `nesidx01` |
| Document header | 16 bytes: `[data_size:4BE][seq:4BE][time_f64:8BE]` |
| Document footer | 8 bytes: `[data_size:4BE][separator:4]` |
| Separator | `\x00\x00\x1e\n` |
| Alignment | 4 bytes (footer starts on 4-byte boundary) |
| Index entry | 16 bytes: `[number:4LE][position:4LE][size:4LE][partition:4LE]` |

## Example output (N = 10 000, warm page cache)

```
=== Rust mmap benchmark  (N = 10000) ===

  write — Vec<u8> + bulk write (buffered)           ~3 500 000 ops/s
  write — pre-allocated mmap (direct map write)     ~1 700 000 ops/s

  forward full scan  (JSON parse per doc)           ~2 400 000 ops/s
  forward full scan  (header only, no JSON parse)   ~40 000 000 ops/s
  backward full scan (JSON parse per doc)           ~2 200 000 ops/s
  range scan 1/3..2/3 (JSON parse per doc)          ~2 400 000 ops/s
  index point-lookup every 10th (JSON parse)        ~2 200 000 ops/s
```

### Comparison to the Node.js benchmarks

The Node.js `bench-read-scenarios.js` benchmarks report "complete scans per second" of the
full 10 000-event store via the `EventStore` API (≈ 40 ops/s forward scan after mmap).

Converting the Rust numbers to the same unit:

| Operation | Node.js mmap | Rust mmap | Ratio |
|---|---|---|---|
| forward full scan | ~42 scans/s | ~240 scans/s | **~5.7×** |
| backward full scan | ~39 scans/s | ~220 scans/s | **~5.6×** |

The remaining gap after removing the `toString()` copy is mainly:
- `serde_json` still allocates for nested objects/arrays (use `serde_json::RawValue` or a
  streaming parser to reduce further)
- The `EventStore` layer in Node.js does additional index and stream-filtering work not
  present in this partition-level benchmark
- CPU branch predictor / TLB warm-up differences
