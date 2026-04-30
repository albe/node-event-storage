/**
 * Scalability benchmark for node-event-storage
 *
 * Measures write ms/op, read ms/op, and startup time (open ms) as the number
 * of partitions and secondary indexes grows.
 *
 * Two scenarios are exercised:
 *
 *   Scenario A – growing partitions, one index per partition
 *     Size steps: 100 / 1000 / 5000 / 10000 / 20000 partitions
 *     Each partition contains 20 documents (~150 bytes each).
 *     One secondary index per partition (N indexes for N partitions).
 *
 *   Scenario B – fixed partitions (100), growing index count
 *     Size steps: 100 / 1000 / 5000 indexes
 *     20 documents per index are guaranteed across the 100 partitions.
 *
 * Metrics collected at each size step
 *   - startup_ms   – time to open a pre-populated storage, including scanning
 *                    the data directory for all partition files plus opening all
 *                    secondary index files one by one.
 *   - write_ms_op  – average ms per write() call (100-op sample), with all N
 *                    secondary indexes registered so every write checks all N
 *                    matchers and updates the one that matches.
 *   - read_ms_op   – average ms per read() call (100-op sample at evenly-spread
 *                    positions through the primary index).
 *
 * Output: a formatted table per scenario showing raw values and absolute /
 * relative change between successive size steps.
 *
 * ──────────────────────────────────────────────────────────────────────────────
 * Technical constraints / known limits
 * ──────────────────────────────────────────────────────────────────────────────
 *
 * 1. Open file descriptors (fds)
 *    Storage opens one fd per partition and one fd per index simultaneously.
 *    Scenario A at 20 000 partitions needs ≈ 40 001 fds (20 000 partitions
 *    + 20 000 index files + 1 primary index).  This benchmark reads the
 *    current soft limit and skips size steps that would require more than 90 %
 *    of that limit, printing a warning.  On most modern Linux systems the soft
 *    limit is already set to 65 536 (sufficient for all steps ≤ 20 000).
 *
 * 2. Directory entry count
 *    Most filesystems support millions of files per directory.  However, the
 *    `readdir` scan inside Storage.scanPartitions() is proportional to the
 *    number of files, so startup time grows at least linearly.  Scenario A at
 *    20 000 partitions creates ≈ 40 001 files in a single directory.
 *
 * 3. Population strategy (secondary indexes)
 *    Secondary index files are created as empty shell files (metadata header
 *    only, no historical data) during the setup phase.  This keeps population
 *    time O(docs + indexes) instead of the quadratic O(docs × indexes) that
 *    results from using Storage.ensureIndex(…, reindex=true) for each index.
 *    The write benchmark still measures the full O(N) per-write cost because
 *    every write() call iterates all N registered secondary-index matchers to
 *    find the matching one.  In a real application that starts fresh (no
 *    historical events), the behaviour is identical.
 *
 * 4. Total data written (Scenario B)
 *    With M indexes and 20 docs/index, partition data grows to:
 *      100 partitions × ceil(M×20/100) docs/partition × ~200 bytes ≈
 *       100 / 1 000 indexes:  ~  2 MB /  20 MB   partition data
 *       5 000 indexes:        ~ 100 MB             partition data
 *    Ensure the target --data-dir has enough free space.
 *
 * Usage:
 *   node bench/bench-scalability.js [--data-dir <path>] [--keep-data]
 *
 *   --data-dir   Where to store temporary benchmark data.
 *                Default: /tmp/bench-scalability
 *   --keep-data  Skip cleanup after the run (useful for manual inspection).
 */

'use strict';

import fs from 'fs';
import path from 'path';
import { execSync } from 'child_process';
import { Storage, LOCK_RECLAIM } from '../index.js';

// ──────────────────────────────────────────────────────────────────────────────
// CLI flags
// ──────────────────────────────────────────────────────────────────────────────
const args = process.argv.slice(2);
const dataDirFlag = args.indexOf('--data-dir');
const BASE_DIR    = dataDirFlag >= 0
    ? path.resolve(args[dataDirFlag + 1])
    : '/tmp/bench-scalability';
const KEEP_DATA = args.includes('--keep-data');

// ──────────────────────────────────────────────────────────────────────────────
// File-descriptor soft limit
// ──────────────────────────────────────────────────────────────────────────────
function getFdSoftLimit() {
    try {
        return parseInt(execSync('bash -c "ulimit -Sn"', { encoding: 'utf8' }).trim(), 10);
    } catch {
        return 65536;
    }
}
const FD_LIMIT = getFdSoftLimit();

// ──────────────────────────────────────────────────────────────────────────────
// Constants
// ──────────────────────────────────────────────────────────────────────────────
const DOCS_PER_PARTITION = 20;
const WRITE_SAMPLE_OPS   = 100;
const READ_SAMPLE_OPS    = 100;

// Pad the data field so the serialised JSON document is roughly 150 bytes.
const DATA_PAD = 'benchmarkpayload_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx';
const APPROX_DOC_SIZE = JSON.stringify({ type: 0, partitionId: 0, data: DATA_PAD, ts: 0 }).length;

// ──────────────────────────────────────────────────────────────────────────────
// Scenario definitions
// ──────────────────────────────────────────────────────────────────────────────
const SCENARIO_A_STEPS      = [100, 1000, 5000, 10000, 20000];
const SCENARIO_B_STEPS      = [100, 1000, 5000];
const SCENARIO_B_PARTITIONS = 100;

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

function rmrf(dir) {
    if (fs.existsSync(dir)) {
        fs.rmSync(dir, { recursive: true, force: true });
    }
}

function elapsed(start) {
    const [s, ns] = process.hrtime(start);
    return s * 1e3 + ns / 1e6;
}

function makeStorageConfig(dataDir, partitioner) {
    return {
        dataDirectory:           dataDir,
        indexDirectory:          dataDir,
        partitioner,
        writeBufferSize:         4096,
        maxWriteBufferDocuments: 0,
        syncOnFlush:             false,
        dirtyReads:              true,
        lock:                    LOCK_RECLAIM,
    };
}

/**
 * Estimate file descriptors needed: one per partition, one per secondary index,
 * plus one for the primary index.
 */
function estimateFds(numPartitions, numIndexes) {
    return numPartitions + numIndexes + 1;
}

/**
 * Populate a fresh data directory:
 *   1. Write all documents directly to partitions (no secondary indexes
 *      registered → O(totalDocs) instead of O(totalDocs × numIndexes)).
 *   2. Create secondary index files as empty shells (header + matcher
 *      metadata, no historical entries) so that startup and write benchmarks
 *      see a realistic file-count overhead without quadratic setup time.
 *
 * @param {string} dataDir
 * @param {number} numPartitions
 * @param {number} numIndexes
 * @param {number} docsPerPartition
 */
function populateStorage(dataDir, numPartitions, numIndexes, docsPerPartition) {
    const partitioner = (doc) => String(doc.partitionId);
    const storage = new Storage('bench', makeStorageConfig(dataDir, partitioner));
    storage.open();

    // Write all documents with NO secondary indexes registered.
    const totalDocs = numPartitions * docsPerPartition;
    for (let seq = 0; seq < totalDocs; seq++) {
        const p      = seq % numPartitions;
        const typeId = numIndexes > 0 ? seq % numIndexes : 0;
        storage.write({ type: typeId, partitionId: p, data: DATA_PAD, ts: Date.now() });
    }
    storage.flush();

    // Create empty secondary index files (reindex=false → no doc scanning).
    // This registers the file on disk so it can be opened in the benchmark.
    for (let i = 0; i < numIndexes; i++) {
        storage.ensureIndex(`idx-${i}`, { type: i }, false);
    }
    storage.flush();
    storage.close();
}

/**
 * Measure startup time: open a pre-populated storage (directory scan +
 * primary index load) and then explicitly open all secondary index files,
 * mirroring what a real application does on boot.
 *
 * @returns {number} Total elapsed milliseconds.
 */
function measureStartup(dataDir, numIndexes) {
    const partitioner = (doc) => String(doc.partitionId);
    const t0 = process.hrtime();
    const storage = new Storage('bench', makeStorageConfig(dataDir, partitioner));
    storage.open();
    for (let i = 0; i < numIndexes; i++) {
        storage.openIndex(`idx-${i}`);
    }
    const ms = elapsed(t0);
    storage.close();
    return ms;
}

/**
 * Measure write performance after all secondary indexes are registered.
 *
 * Setup (opening indexes) is outside the timed region.
 * Each write() call checks all N secondary-index matchers to find the
 * matching one — this O(N) loop is the dominant cost for large N.
 *
 * @returns {number} Average ms per write() call.
 */
function measureWrite(dataDir, numPartitions, numIndexes) {
    const partitioner = (doc) => String(doc.partitionId);
    const storage = new Storage('bench', makeStorageConfig(dataDir, partitioner));
    storage.open();

    // Open all secondary indexes outside the timed region.
    for (let i = 0; i < numIndexes; i++) {
        storage.openIndex(`idx-${i}`);
    }

    const t0 = process.hrtime();
    for (let i = 0; i < WRITE_SAMPLE_OPS; i++) {
        const p      = i % numPartitions;
        const typeId = numIndexes > 0 ? i % numIndexes : 0;
        storage.write({ type: typeId, partitionId: p, data: DATA_PAD, ts: Date.now() });
    }
    storage.flush();
    const ms = elapsed(t0);

    storage.close();
    return ms / WRITE_SAMPLE_OPS;
}

/**
 * Measure read performance using evenly-spread positions in the primary index.
 *
 * @returns {number} Average ms per read() call.
 */
function measureRead(dataDir) {
    const partitioner = (doc) => String(doc.partitionId);
    const storage = new Storage('bench', makeStorageConfig(dataDir, partitioner));
    storage.open();

    const length = storage.length;
    const step   = Math.max(1, Math.floor(length / READ_SAMPLE_OPS));

    const t0 = process.hrtime();
    for (let i = 0; i < READ_SAMPLE_OPS; i++) {
        const pos = ((i * step) % length) + 1;
        storage.read(pos);
    }
    const ms = elapsed(t0);

    storage.close();
    return ms / READ_SAMPLE_OPS;
}

// ──────────────────────────────────────────────────────────────────────────────
// Table printing
// ──────────────────────────────────────────────────────────────────────────────

function fmtMs(v)  { return v.toFixed(3); }
function fmtSign(v, d = 3) { return (v >= 0 ? '+' : '') + v.toFixed(d); }
function fmtPct(v) { return (v >= 0 ? '+' : '') + v.toFixed(1) + '%'; }
function fmtDelta(abs, pct) { return `${fmtSign(abs)} (${fmtPct(pct)})`; }

const COL = { size: 12, metric: 14, delta: 22 };

function pad(s, n)  { return String(s).padStart(n); }
function lpad(s, n) { return String(s).padEnd(n); }

/**
 * @param {string}   title
 * @param {string}   sizeLabel
 * @param {Array<{size:number, startup_ms:number, write_ms_op:number, read_ms_op:number}>} rows
 */
function printTable(title, sizeLabel, rows) {
    if (rows.length === 0) {
        console.log(`\n${title}`);
        console.log('  (no results — all steps skipped due to fd constraints)');
        return;
    }
    const lineWidth = COL.size + (COL.metric + COL.delta) * 3 + 6;
    const HR = '─'.repeat(lineWidth);

    console.log('\n' + title);
    console.log(HR);
    console.log(
        lpad(sizeLabel,     COL.size) +
        pad('startup(ms)',  COL.metric) + pad('Δstartup',    COL.delta) +
        pad('write(ms/op)', COL.metric) + pad('Δwrite',      COL.delta) +
        pad('read(ms/op)',  COL.metric) + pad('Δread',       COL.delta)
    );
    console.log(HR);

    for (let i = 0; i < rows.length; i++) {
        const r    = rows[i];
        const prev = i > 0 ? rows[i - 1] : null;

        const startupDelta = prev
            ? fmtDelta(r.startup_ms  - prev.startup_ms,
                       (r.startup_ms  - prev.startup_ms)  / prev.startup_ms  * 100)
            : '—';
        const writeDelta = prev
            ? fmtDelta(r.write_ms_op - prev.write_ms_op,
                       (r.write_ms_op - prev.write_ms_op) / prev.write_ms_op * 100)
            : '—';
        const readDelta = prev
            ? fmtDelta(r.read_ms_op  - prev.read_ms_op,
                       (r.read_ms_op  - prev.read_ms_op)  / prev.read_ms_op  * 100)
            : '—';

        console.log(
            lpad(r.size,              COL.size) +
            pad(fmtMs(r.startup_ms),  COL.metric) + pad(startupDelta, COL.delta) +
            pad(fmtMs(r.write_ms_op), COL.metric) + pad(writeDelta,   COL.delta) +
            pad(fmtMs(r.read_ms_op),  COL.metric) + pad(readDelta,    COL.delta)
        );
    }
    console.log(HR);
}

// ──────────────────────────────────────────────────────────────────────────────
// Main
// ──────────────────────────────────────────────────────────────────────────────

console.log(`\nnode-event-storage  scalability benchmark`);
console.log(`Node.js ${process.version}   approx. doc size: ${APPROX_DOC_SIZE} bytes`);
console.log(`Data root: ${BASE_DIR}`);
console.log(`fd soft limit: ${FD_LIMIT}`);
console.log(`Write-sample ops: ${WRITE_SAMPLE_OPS}   Read-sample ops: ${READ_SAMPLE_OPS}`);

fs.mkdirSync(BASE_DIR, { recursive: true });

// ─── Scenario A ──────────────────────────────────────────────────────────────
console.log('\n== Scenario A: growing partitions (1 index per partition) ==');
const scenarioAResults = [];

for (const numPartitions of SCENARIO_A_STEPS) {
    const numIndexes       = numPartitions;     // one index per partition
    const docsPerPartition = DOCS_PER_PARTITION;
    const totalDocs        = numPartitions * docsPerPartition;
    const requiredFds      = estimateFds(numPartitions, numIndexes);

    if (requiredFds > FD_LIMIT * 0.9) {
        console.warn(
            `  ⚠  Skipping ${numPartitions} partitions: estimated fds (${requiredFds}) ` +
            `> 90 % of fd soft limit (${FD_LIMIT}).`
        );
        continue;
    }

    const dataDir = path.join(BASE_DIR, `scenario-a-${numPartitions}`);
    rmrf(dataDir);
    fs.mkdirSync(dataDir, { recursive: true });

    process.stdout.write(
        `  [${numPartitions}p / ${numIndexes}idx / ${totalDocs} docs] populate … `
    );
    const tPop = Date.now();
    populateStorage(dataDir, numPartitions, numIndexes, docsPerPartition);
    process.stdout.write(`${Date.now() - tPop} ms | `);

    const startup_ms  = measureStartup(dataDir, numIndexes);
    process.stdout.write(`startup ${fmtMs(startup_ms)} ms | `);

    const write_ms_op = measureWrite(dataDir, numPartitions, numIndexes);
    process.stdout.write(`write ${fmtMs(write_ms_op)} ms/op | `);

    const read_ms_op  = measureRead(dataDir);
    process.stdout.write(`read ${fmtMs(read_ms_op)} ms/op\n`);

    scenarioAResults.push({ size: numPartitions, startup_ms, write_ms_op, read_ms_op });

    if (!KEEP_DATA) rmrf(dataDir);
}

printTable(
    'Scenario A  –  growing partitions, 1 index/partition, 20 docs/partition',
    'Partitions',
    scenarioAResults
);

// ─── Scenario B ──────────────────────────────────────────────────────────────
console.log('\n== Scenario B: 100 partitions, growing index count ==');
const scenarioBResults = [];

for (const numIndexes of SCENARIO_B_STEPS) {
    const numPartitions    = SCENARIO_B_PARTITIONS;
    // Guarantee ≥ 20 docs per index distributed evenly across partitions.
    const docsPerPartition = Math.max(
        DOCS_PER_PARTITION,
        Math.ceil(numIndexes * DOCS_PER_PARTITION / numPartitions)
    );
    const totalDocs    = numPartitions * docsPerPartition;
    const requiredFds  = estimateFds(numPartitions, numIndexes);

    if (requiredFds > FD_LIMIT * 0.9) {
        console.warn(
            `  ⚠  Skipping ${numIndexes} indexes: estimated fds (${requiredFds}) ` +
            `> 90 % of fd soft limit (${FD_LIMIT}).`
        );
        continue;
    }

    const dataDir = path.join(BASE_DIR, `scenario-b-${numIndexes}`);
    rmrf(dataDir);
    fs.mkdirSync(dataDir, { recursive: true });

    process.stdout.write(
        `  [${numPartitions}p / ${numIndexes}idx / ${totalDocs} docs` +
        ` (${docsPerPartition}/p)] populate … `
    );
    const tPop = Date.now();
    populateStorage(dataDir, numPartitions, numIndexes, docsPerPartition);
    process.stdout.write(`${Date.now() - tPop} ms | `);

    const startup_ms  = measureStartup(dataDir, numIndexes);
    process.stdout.write(`startup ${fmtMs(startup_ms)} ms | `);

    const write_ms_op = measureWrite(dataDir, numPartitions, numIndexes);
    process.stdout.write(`write ${fmtMs(write_ms_op)} ms/op | `);

    const read_ms_op  = measureRead(dataDir);
    process.stdout.write(`read ${fmtMs(read_ms_op)} ms/op\n`);

    scenarioBResults.push({ size: numIndexes, startup_ms, write_ms_op, read_ms_op });

    if (!KEEP_DATA) rmrf(dataDir);
}

printTable(
    'Scenario B  –  100 partitions, growing index count (≥ 20 docs/index)',
    'Indexes',
    scenarioBResults
);

console.log('\nDone.');
