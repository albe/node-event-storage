//! # node-event-storage mmap benchmark (Rust)
//!
//! A standalone benchmark that replicates the *exact* on-disk file format used by
//! node-event-storage, but implemented in Rust using memory-mapped I/O.
//!
//! ## File formats (must match the Node.js library exactly so files are cross-readable)
//!
//! ### Partition file (`nesprt03`)
//! ```text
//! [magic      : 8 bytes  "nesprt03"]
//! [meta_size  : 4 bytes  BE u32              ]  ← byte-length of the metadata block below
//! [metadata   : meta_size bytes              ]  ← JSON string + space-padding + '\n'
//!                                                  total (8+4+meta_size) is a multiple of 16
//! for each document:
//!   [data_size : 4 bytes  BE u32]              ← byte-length of the UTF-8 payload
//!   [seq_num   : 4 bytes  BE u32]              ← monotone sequence number
//!   [time_us   : 8 bytes  BE f64]              ← microseconds since epoch (as raw f64 bits)
//!   [data      : data_size bytes]              ← UTF-8 JSON payload
//!   [padding   : alignTo(data_size+8, 4) bytes of ' ']
//!   [data_size : 4 bytes  BE u32]              ← footer: same as header data_size (for O(1) backward scan)
//!   [separator : 4 bytes  b"\x00\x00\x1e\n"]
//! ```
//!
//! ### Index file (`nesidx01`)
//! ```text
//! [magic     : 8 bytes "nesidx01"]
//! [meta_size : 4 bytes BE u32    ]
//! [metadata  : meta_size bytes   ]
//! for each entry (16 bytes, all u32 LE):
//!   [number    : 4 bytes LE u32]  ← global sequence number
//!   [position  : 4 bytes LE u32]  ← byte offset from partition data start
//!   [size      : 4 bytes LE u32]  ← payload byte-length
//!   [partition : 4 bytes LE u32]  ← partition ID (0 here)
//! ```
//!
//! ## Why Rust + mmap can outperform Node.js + mmap
//!
//! In Node.js, *every* document read calls `buffer.toString('utf8', start, end)`, which
//! allocates a new V8 string and copies the bytes regardless of whether the backing buffer
//! is mmap'd or not.  V8 strings are immutable and GC-managed — there is no way to hand
//! the engine a raw pointer into mapped memory.
//!
//! In Rust, `serde_json::from_slice(&mmap[start..end])` parses JSON *directly* from the
//! mapped region.  String fields that are only inspected (not stored) are never heap-allocated;
//! `serde_json::Value` borrows from the slice for the duration of the parse.  The net result
//! is zero extra allocations for the common "read a field, make a decision" hot path.
//!
//! Additionally:
//! - `u32::from_be_bytes(...)` and `f64::from_bits(...)` are single-instruction reads with no
//!   overhead beyond the memory access itself.
//! - O(1) backward traversal using the footer (`data_size` written just before the separator)
//!   avoids any `lastIndexOf` scan.
//! - The Rust allocator is far cheaper than V8's GC for any remaining small allocations.

use std::fs::File;
use std::io;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use memmap2::{Mmap, MmapMut, MmapOptions};

// ── File format constants ────────────────────────────────────────────────────

const PARTITION_MAGIC: &[u8; 8] = b"nesprt03";
const INDEX_MAGIC: &[u8; 8] = b"nesidx01";

/// Four-byte record separator written after every document.
const DOCUMENT_SEPARATOR: &[u8; 4] = b"\x00\x00\x1e\n";

/// Fixed 16-byte document header: [data_size:4BE][seq:4BE][time_f64:8BE]
const DOCUMENT_HEADER_SIZE: usize = 16;

/// Fixed footer appended after padding: [data_size:4BE][separator:4]
const DOCUMENT_FOOTER_SIZE: usize = 4 + 4;

/// All documents are padded so that (data_size + DOCUMENT_FOOTER_SIZE) is a
/// multiple of DOCUMENT_ALIGNMENT, matching `alignTo` in util.js.
const DOCUMENT_ALIGNMENT: usize = 4;

/// Each index entry is 16 bytes (four u32 LE values).
const INDEX_ENTRY_SIZE: usize = 16;

// ── Layout helpers ───────────────────────────────────────────────────────────

/// How many pad bytes follow the data so the footer starts on an aligned boundary.
///
/// Mirrors `alignTo(dataSize + DOCUMENT_FOOTER_SIZE, DOCUMENT_ALIGNMENT)` in util.js.
#[inline]
fn pad_size(data_size: usize) -> usize {
    (DOCUMENT_ALIGNMENT - ((data_size + DOCUMENT_FOOTER_SIZE) % DOCUMENT_ALIGNMENT))
        % DOCUMENT_ALIGNMENT
}

/// Total on-disk byte-length of one document record.
///
/// Mirrors `documentWriteSize(dataSize)` in ReadablePartition.js.
#[inline]
fn document_write_size(data_size: usize) -> usize {
    DOCUMENT_HEADER_SIZE + data_size + pad_size(data_size) + DOCUMENT_FOOTER_SIZE
}

// ── File-header builder ──────────────────────────────────────────────────────

/// Build a file header that matches `buildMetadataHeader(magic, metadata)` in util.js.
///
/// Layout: `[magic:8][meta_size:4 BE][metadata_json + spaces + '\n']`
/// The total length is always a multiple of 16.
fn build_file_header(magic: &[u8; 8], metadata_json: &str) -> Vec<u8> {
    let meta_bytes = metadata_json.as_bytes();
    let meta_len = meta_bytes.len();
    // pad so that 8 + 4 + meta_len + 1_newline is divisible by 16
    let pad = (16 - ((8 + 4 + meta_len + 1) % 16)) % 16;
    let meta_size = meta_len + pad + 1; // spaces + '\n'

    let mut buf = Vec::with_capacity(8 + 4 + meta_size);
    buf.extend_from_slice(magic);
    buf.extend_from_slice(&(meta_size as u32).to_be_bytes());
    buf.extend_from_slice(meta_bytes);
    buf.extend(std::iter::repeat(b' ').take(pad));
    buf.push(b'\n');
    debug_assert_eq!(buf.len() % 16, 0);
    buf
}

// ── Document serialiser ──────────────────────────────────────────────────────

/// Append one document record to `out`, returning the number of bytes appended.
fn append_document(out: &mut Vec<u8>, data: &[u8], seq: u32, time_us: f64) -> usize {
    let data_size = data.len();
    let written_before = out.len();

    // Header
    out.extend_from_slice(&(data_size as u32).to_be_bytes());
    out.extend_from_slice(&seq.to_be_bytes());
    out.extend_from_slice(&time_us.to_bits().to_be_bytes());
    // Payload
    out.extend_from_slice(data);
    // Alignment padding (spaces)
    out.extend(std::iter::repeat(b' ').take(pad_size(data_size)));
    // Footer
    out.extend_from_slice(&(data_size as u32).to_be_bytes());
    out.extend_from_slice(DOCUMENT_SEPARATOR);

    out.len() - written_before
}

/// Append one index entry to `out`.
fn append_index_entry(out: &mut Vec<u8>, number: u32, position: u32, size: u32, partition: u32) {
    out.extend_from_slice(&number.to_le_bytes());
    out.extend_from_slice(&position.to_le_bytes());
    out.extend_from_slice(&size.to_le_bytes());
    out.extend_from_slice(&partition.to_le_bytes());
}

// ── Partition reader ─────────────────────────────────────────────────────────

/// A read-only view over a memory-mapped partition file.
struct PartitionReader {
    mmap: Mmap,
    /// Byte offset within the mmap where document data starts (after the file header).
    header_size: usize,
    /// Total bytes of document data (= mmap.len() - header_size).
    data_size: usize,
}

impl PartitionReader {
    fn open(path: &str) -> io::Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        assert_eq!(&mmap[..8], PARTITION_MAGIC, "not a valid partition file");
        let meta_size = u32::from_be_bytes(mmap[8..12].try_into().unwrap()) as usize;
        let header_size = 8 + 4 + meta_size;
        let data_size = mmap.len() - header_size;
        Ok(Self { mmap, header_size, data_size })
    }

    /// Read the document at the given data-relative `position`.
    ///
    /// Returns `(payload_slice, seq_num, time_us)` where `payload_slice` is a zero-copy
    /// borrow directly into the mmap — no heap allocation.
    #[inline]
    fn read_at(&self, position: usize) -> Option<(&[u8], u32, f64)> {
        if position + DOCUMENT_HEADER_SIZE > self.data_size {
            return None;
        }
        let abs = self.header_size + position;
        let m = &self.mmap;

        let data_size = u32::from_be_bytes(m[abs..abs + 4].try_into().unwrap()) as usize;
        if data_size == 0 || data_size > 64 * 1024 * 1024 {
            return None;
        }
        if position + document_write_size(data_size) > self.data_size {
            return None;
        }

        let seq = u32::from_be_bytes(m[abs + 4..abs + 8].try_into().unwrap());
        let time_us = f64::from_bits(u64::from_be_bytes(m[abs + 8..abs + 16].try_into().unwrap()));

        let payload = &m[abs + DOCUMENT_HEADER_SIZE..abs + DOCUMENT_HEADER_SIZE + data_size];
        Some((payload, seq, time_us))
    }

    /// O(1) backward step: given that `pos` is the data-relative end of the current
    /// document (i.e. the position *after* its separator), return the start of that
    /// document using the footer stored just before the separator.
    ///
    /// Mirrors the "exact boundary" fast path in `findDocumentPositionBefore` in
    /// ReadablePartition.js, but made the primary (only) path here because the
    /// footer is always written and the file format guarantees it.
    #[inline]
    fn doc_start_before(&self, pos: usize) -> Option<usize> {
        let abs = self.header_size + pos;
        let sep = DOCUMENT_SEPARATOR.len(); // 4
        if abs < sep + 4 {
            return None;
        }
        // Verify the separator is intact (sanity / corruption check).
        debug_assert_eq!(
            &self.mmap[abs - sep..abs],
            DOCUMENT_SEPARATOR.as_slice(),
            "missing separator at pos {pos}"
        );
        // The 4 bytes immediately before the separator are the footer data_size.
        let footer_start = abs - sep - 4;
        let data_size =
            u32::from_be_bytes(self.mmap[footer_start..footer_start + 4].try_into().unwrap())
                as usize;
        let write_size = document_write_size(data_size);
        if pos < write_size {
            return None;
        }
        Some(pos - write_size)
    }

    /// Iterator that yields `(position, payload, seq)` in forward order.
    fn iter_forward(&self) -> ForwardIter<'_> {
        ForwardIter { reader: self, position: 0 }
    }

    /// Iterator that yields `(position, payload, seq)` in reverse order.
    fn iter_backward(&self) -> BackwardIter<'_> {
        BackwardIter { reader: self, pos: self.data_size }
    }
}

// Forward iterator over all documents in a partition.
struct ForwardIter<'a> {
    reader: &'a PartitionReader,
    position: usize,
}

impl<'a> Iterator for ForwardIter<'a> {
    type Item = (usize, &'a [u8], u32);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let (payload, seq, _) = self.reader.read_at(self.position)?;
        let pos = self.position;
        self.position += document_write_size(payload.len());
        Some((pos, payload, seq))
    }
}

// Backward iterator — uses the O(1) footer-based step.
struct BackwardIter<'a> {
    reader: &'a PartitionReader,
    /// data-relative end of the *next* document to yield (starts at data_size).
    pos: usize,
}

impl<'a> Iterator for BackwardIter<'a> {
    type Item = (usize, &'a [u8], u32);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.pos == 0 {
            return None;
        }
        let doc_start = self.reader.doc_start_before(self.pos)?;
        let (payload, seq, _) = self.reader.read_at(doc_start)?;
        self.pos = doc_start;
        Some((doc_start, payload, seq))
    }
}

// ── Index reader ─────────────────────────────────────────────────────────────

struct IndexReader {
    mmap: Mmap,
    header_size: usize,
    count: usize,
}

impl IndexReader {
    fn open(path: &str) -> io::Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        assert_eq!(&mmap[..8], INDEX_MAGIC.as_slice(), "not a valid index file");
        let meta_size = u32::from_be_bytes(mmap[8..12].try_into().unwrap()) as usize;
        let header_size = 8 + 4 + meta_size;
        let count = (mmap.len() - header_size) / INDEX_ENTRY_SIZE;
        Ok(Self { mmap, header_size, count })
    }

    /// Return the index entry at zero-based `idx`.
    #[inline]
    fn get(&self, idx: usize) -> (u32, u32, u32, u32) {
        let off = self.header_size + idx * INDEX_ENTRY_SIZE;
        let m = &self.mmap;
        let number = u32::from_le_bytes(m[off..off + 4].try_into().unwrap());
        let position = u32::from_le_bytes(m[off + 4..off + 8].try_into().unwrap());
        let size = u32::from_le_bytes(m[off + 8..off + 12].try_into().unwrap());
        let partition = u32::from_le_bytes(m[off + 12..off + 16].try_into().unwrap());
        (number, position, size, partition)
    }
}

// ── mmap-write helpers ───────────────────────────────────────────────────────

/// Pre-allocate `path` to exactly `size` bytes and return a writable mmap over it.
fn create_mmap_write(path: &str, size: u64) -> io::Result<MmapMut> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    file.set_len(size)?;
    Ok(unsafe { MmapOptions::new().map_mut(&file)? })
}

// ── Timing helpers ───────────────────────────────────────────────────────────

fn now_us() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as f64
}

fn print_ops(label: &str, n: usize, elapsed: Duration) {
    let ops = n as f64 / elapsed.as_secs_f64();
    println!("  {:<45}  {:>10.0} ops/s  ({:.3} ms/op)", label, ops, 1000.0 / ops);
}

// ── Main ─────────────────────────────────────────────────────────────────────

fn main() -> io::Result<()> {
    let n: usize =
        std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(10_000);

    let dir = "/tmp/nes-bench-rust";
    std::fs::create_dir_all(dir)?;
    let partition_path = format!("{dir}/partition.data");
    let index_path = format!("{dir}/partition.index");

    // Document body matching bench-read-scenarios.js:
    //   { type: 'SomeEvent', payload: 'x'.repeat(460), seq: N }
    // At N=0 this is  {"type":"SomeEvent","payload":"xxx...","seq":0}
    // which is 8+4+3+10+3+460+6+1+N_digits = ~502 bytes (grows slightly as seq grows).
    let payload_filler = "x".repeat(460);

    // ── WRITE benchmark ──────────────────────────────────────────────────────
    // Strategy A: build everything in a Vec<u8> then write atomically.
    // This is what most high-performance log writers do: batch into memory then
    // flush with a single syscall, identical in spirit to WritablePartition's
    // write-buffer approach in Node.js.
    println!("\n=== Rust mmap benchmark  (N = {n}) ===\n");

    let (partition_header_size, index_header_size);
    {
        let part_header_bytes = build_file_header(PARTITION_MAGIC, r#"{"epoch":1577836800000}"#);
        let idx_header_bytes =
            build_file_header(INDEX_MAGIC, r#"{"entryClass":"Entry","entrySize":16}"#);
        partition_header_size = part_header_bytes.len();
        index_header_size = idx_header_bytes.len();

        // Estimate total partition size (upper bound; all docs same payload length
        // so the estimate is exact for a fixed N).
        let sample_doc = format!(
            r#"{{"type":"SomeEvent","payload":"{payload_filler}","seq":{}}}"#,
            n - 1
        );
        let max_doc_bytes = sample_doc.len();
        let max_write_size = document_write_size(max_doc_bytes);

        // --- A: Vec<u8> + single write (baseline write strategy) ---------------
        let t_write_vec = {
            let mut part_buf: Vec<u8> = Vec::with_capacity(partition_header_size + n * max_write_size);
            let mut idx_buf: Vec<u8> = Vec::with_capacity(index_header_size + n * INDEX_ENTRY_SIZE);
            part_buf.extend_from_slice(&part_header_bytes);
            idx_buf.extend_from_slice(&idx_header_bytes);

            let t0 = Instant::now();
            let mut position: usize = 0;
            for i in 0..n {
                let doc = format!(
                    r#"{{"type":"SomeEvent","payload":"{payload_filler}","seq":{i}}}"#
                );
                let doc_bytes = doc.as_bytes();
                let data_size = doc_bytes.len();
                append_document(&mut part_buf, doc_bytes, i as u32, now_us());
                append_index_entry(&mut idx_buf, i as u32, position as u32, data_size as u32, 0);
                position += document_write_size(data_size);
            }
            let elapsed = t0.elapsed();

            std::fs::write(&partition_path, &part_buf)?;
            std::fs::write(&index_path, &idx_buf)?;
            elapsed
        };
        print_ops("write — Vec<u8> + bulk write (buffered)", n, t_write_vec);

        // --- B: pre-allocated mmap write ---------------------------------------
        // Know the exact file sizes because all docs are the same doc length up
        // to the last few bytes (seq digits).  Use the overestimate; then truncate.
        let part_total: u64 = (partition_header_size + n * max_write_size) as u64;
        let idx_total: u64 = (index_header_size + n * INDEX_ENTRY_SIZE) as u64;

        let t_write_mmap = {
            let mut part_mmap = create_mmap_write(&partition_path, part_total)?;
            let mut idx_mmap = create_mmap_write(&index_path, idx_total)?;

            // Write headers
            part_mmap[..partition_header_size].copy_from_slice(&part_header_bytes);
            idx_mmap[..index_header_size].copy_from_slice(&idx_header_bytes);

            let t0 = Instant::now();
            let mut part_cursor = partition_header_size;
            let mut idx_cursor = index_header_size;
            let mut data_position: usize = 0;

            for i in 0..n {
                let doc = format!(
                    r#"{{"type":"SomeEvent","payload":"{payload_filler}","seq":{i}}}"#
                );
                let doc_bytes = doc.as_bytes();
                let data_size = doc_bytes.len();
                let write_size = document_write_size(data_size);

                // Write document directly into mmap
                let d = &mut part_mmap[part_cursor..part_cursor + write_size];
                d[..4].copy_from_slice(&(data_size as u32).to_be_bytes());
                d[4..8].copy_from_slice(&(i as u32).to_be_bytes());
                d[8..16].copy_from_slice(&now_us().to_bits().to_be_bytes());
                d[DOCUMENT_HEADER_SIZE..DOCUMENT_HEADER_SIZE + data_size]
                    .copy_from_slice(doc_bytes);
                let pad = pad_size(data_size);
                let footer_off = DOCUMENT_HEADER_SIZE + data_size + pad;
                // space-fill padding
                d[DOCUMENT_HEADER_SIZE + data_size..footer_off].fill(b' ');
                d[footer_off..footer_off + 4].copy_from_slice(&(data_size as u32).to_be_bytes());
                d[footer_off + 4..footer_off + 8].copy_from_slice(DOCUMENT_SEPARATOR);

                // Write index entry directly into mmap
                let e = &mut idx_mmap[idx_cursor..idx_cursor + INDEX_ENTRY_SIZE];
                e[..4].copy_from_slice(&(i as u32).to_le_bytes());
                e[4..8].copy_from_slice(&(data_position as u32).to_le_bytes());
                e[8..12].copy_from_slice(&(data_size as u32).to_le_bytes());
                e[12..16].copy_from_slice(&0u32.to_le_bytes());

                part_cursor += write_size;
                idx_cursor += INDEX_ENTRY_SIZE;
                data_position += write_size;
            }
            let elapsed = t0.elapsed();

            // Flush mapped pages to disk and truncate to actual size
            part_mmap.flush()?;
            idx_mmap.flush()?;
            drop(part_mmap);
            drop(idx_mmap);
            // Truncate to exact written length (mmap was over-allocated)
            {
                let f = std::fs::OpenOptions::new().write(true).open(&partition_path)?;
                f.set_len(part_cursor as u64)?;
            }
            {
                let f = std::fs::OpenOptions::new().write(true).open(&index_path)?;
                f.set_len(idx_cursor as u64)?;
            }
            elapsed
        };
        print_ops("write — pre-allocated mmap (direct map write)", n, t_write_mmap);
    }

    // ── READ benchmarks ───────────────────────────────────────────────────────
    let rounds = 5usize;

    println!();

    // Warmup: one full forward pass before we start measuring.
    {
        let reader = PartitionReader::open(&partition_path)?;
        let mut count = 0usize;
        for (_, data, _) in reader.iter_forward() {
            let _: serde_json::Value = serde_json::from_slice(data).unwrap();
            count += 1;
        }
        assert_eq!(count, n, "warmup: expected {n} docs, got {count}");
    }

    // 1. Forward full scan — parse every document as serde_json::Value.
    {
        let mut total = Duration::ZERO;
        for _ in 0..rounds {
            let reader = PartitionReader::open(&partition_path)?;
            let t0 = Instant::now();
            let mut count = 0usize;
            for (_, data, _) in reader.iter_forward() {
                // serde_json::from_slice parses directly from &mmap[..] — no String copy.
                let _v: serde_json::Value = serde_json::from_slice(data).unwrap();
                count += 1;
            }
            total += t0.elapsed();
            assert_eq!(count, n);
        }
        print_ops("forward full scan  (JSON parse per doc)", n, total / rounds as u32);
    }

    // 2. Forward full scan — no JSON parse, just iterate + count headers.
    //    Represents the overhead of mmap traversal itself.
    {
        let mut total = Duration::ZERO;
        for _ in 0..rounds {
            let reader = PartitionReader::open(&partition_path)?;
            let t0 = Instant::now();
            let mut count = 0usize;
            for (_, _data, _seq) in reader.iter_forward() {
                count += 1;
            }
            total += t0.elapsed();
            assert_eq!(count, n);
        }
        print_ops("forward full scan  (header only, no JSON parse)", n, total / rounds as u32);
    }

    // 3. Backward full scan — parse every document.
    {
        let mut total = Duration::ZERO;
        for _ in 0..rounds {
            let reader = PartitionReader::open(&partition_path)?;
            let t0 = Instant::now();
            let mut count = 0usize;
            for (_, data, _) in reader.iter_backward() {
                let _v: serde_json::Value = serde_json::from_slice(data).unwrap();
                count += 1;
            }
            total += t0.elapsed();
            assert_eq!(count, n, "backward: expected {n} docs, got {count}");
        }
        print_ops("backward full scan (JSON parse per doc)", n, total / rounds as u32);
    }

    // 4. Range scan — middle third (mirrors bench-read-scenarios.js test 4).
    {
        // Collect all positions with a single forward pass.
        let all_positions: Vec<usize> = {
            let reader = PartitionReader::open(&partition_path)?;
            reader.iter_forward().map(|(pos, _, _)| pos).collect()
        };
        let from_idx = n / 3;
        let to_idx = 2 * n / 3;
        let range_start = all_positions[from_idx];
        let range_n = to_idx - from_idx;

        let mut total = Duration::ZERO;
        for _ in 0..rounds {
            let reader = PartitionReader::open(&partition_path)?;
            let t0 = Instant::now();
            let mut count = 0usize;
            let mut pos = range_start;
            while count < range_n {
                match reader.read_at(pos) {
                    Some((data, _, _)) => {
                        let data_size = data.len();
                        let _v: serde_json::Value = serde_json::from_slice(data).unwrap();
                        count += 1;
                        pos += document_write_size(data_size);
                    }
                    None => break,
                }
            }
            total += t0.elapsed();
            assert_eq!(count, range_n);
        }
        print_ops("range scan 1/3..2/3 (JSON parse per doc)", range_n, total / rounds as u32);
    }

    // 5. Index point-lookup — look up every 10th document via the index.
    {
        let index = IndexReader::open(&index_path)?;
        let reader = PartitionReader::open(&partition_path)?;
        let probe_n = n / 10;

        let mut total = Duration::ZERO;
        for _ in 0..rounds {
            let t0 = Instant::now();
            for step in 0..probe_n {
                let i = step * 10;
                let (_number, position, _size, _partition) = index.get(i);
                let (data, _seq, _time) = reader.read_at(position as usize).unwrap();
                let _v: serde_json::Value = serde_json::from_slice(data).unwrap();
            }
            total += t0.elapsed();
        }
        print_ops("index point-lookup every 10th (JSON parse)", probe_n, total / rounds as u32);
    }

    // ── File sizes ────────────────────────────────────────────────────────────
    let part_bytes = std::fs::metadata(&partition_path)?.len();
    let idx_bytes = std::fs::metadata(&index_path)?.len();
    println!();
    println!(
        "  Partition: {} KB   Index: {} KB   (files are binary-compatible with node-event-storage)",
        part_bytes / 1024,
        idx_bytes / 1024
    );
    println!();

    Ok(())
}
