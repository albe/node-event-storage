/**
 * Memory consumption stress test – mocha spec.
 *
 * Phase 1 (Write): Measures process memory at startup (before EventStore
 * initialisation), inside the 'ready' callback, and after writing 1 000,
 * 2 000 and 10 000 events.
 *
 * Phase 2 (Read): Opens the same store in read-only mode and measures memory
 * before initialisation, inside the 'ready' callback and after iterating all
 * previously written events.
 *
 * Phase 3 (Open/Close Leak Detection): Runs 100 open/close cycles for each
 * of the following components and asserts that heap growth (heapUsed) stays
 * within acceptable bounds, confirming that no objects are silently retained:
 *   - Index   : direct ReadOnlyIndex open/close cycles
 *   - Partition: direct ReadOnlyPartition open/close cycles
 *   - Stream  : EventStore write + closeEventStream (make stream read-only)
 *   - EventStore (writable): open/close the full store lifecycle
 *   - EventStore (read-only): open/close the full store lifecycle with ReadOnlyIndex
 *
 * For more accurate GC measurements you may run with:
 *   node --expose-gc node_modules/.bin/mocha stress-test/memory.spec.js
 */

import path from 'path';
import fs from 'fs';
import os from 'os';

import EventStore from '../index.js';
import Index from '../src/Index.js';
import Partition from '../src/Partition.js';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
const INDEX_ENTRY_BYTES = 16; // 4 × UInt32 – see src/IndexEntry.js

const STREAM_NAME = 'memory-test-stream';

// Write milestones: cumulative event counts at which memory is sampled.
const WRITE_MILESTONES = [1000, 2000, 10000];

// Events per commit batch (trade-off between callback overhead and accuracy).
const BATCH_SIZE = 100;

// How many times larger the growth at 10 000 events is allowed to be compared
// to the growth at 1 000 events.  10 × events → ≤ GROWTH_RATIO_LIMIT × growth.
const GROWTH_RATIO_LIMIT = 15;

// Number of open/close cycles per leak-detection scenario.
const LEAK_CYCLES = 100;

// Maximum acceptable heap growth (heapUsed) per open/close cycle.
// With GC exposed a well-behaved implementation shows near-zero growth;
// without GC the allocator may retain a page or two temporarily.
const LEAK_BYTES_PER_CYCLE_LIMIT = 10 * 1024; // 10 KiB

// ---------------------------------------------------------------------------
// Memory helpers
// ---------------------------------------------------------------------------

/** Optionally trigger GC if the runtime was started with --expose-gc. */
function tryGC() {
    if (typeof global.gc === 'function') global.gc();
}

/** Sample current process memory and return a plain object. */
function sample(label) {
    tryGC();
    const m = process.memoryUsage();
    return {
        label,
        rss:       m.rss,
        heapUsed:  m.heapUsed,
        heapTotal: m.heapTotal,
    };
}

/** Format bytes as a right-aligned MiB string. */
function fmtMB(bytes, width = 8) {
    return (bytes / 1024 / 1024).toFixed(2).padStart(width);
}

/** Format a signed growth value in MiB. */
function fmtGrowth(bytes, width = 9) {
    const mb = bytes / 1024 / 1024;
    const s  = (mb >= 0 ? '+' : '') + mb.toFixed(2);
    return s.padStart(width);
}

/** Print a formatted table of memory samples. */
function printTable(title, samples) {
    const COL = { label: 36, rss: 10, heap: 10, growth: 11 };
    const HR  = '-'.repeat(COL.label + COL.rss + COL.heap + COL.growth + 9);

    console.log(`\n${title}`);
    console.log(HR);
    console.log(
        'Phase'.padEnd(COL.label) +
        'RSS (MiB)'.padStart(COL.rss) + '  ' +
        'Heap (MiB)'.padStart(COL.heap) + '  ' +
        'RSS growth'.padStart(COL.growth)
    );
    console.log(HR);

    const baseline = samples[0];
    for (const s of samples) {
        const growth = s === baseline
            ? '        -'
            : fmtGrowth(s.rss - baseline.rss, COL.growth);
        console.log(
            s.label.padEnd(COL.label) +
            fmtMB(s.rss, COL.rss) + '  ' +
            fmtMB(s.heapUsed, COL.heap) + '  ' +
            growth
        );
    }
    console.log(HR);
}

// ---------------------------------------------------------------------------
// Phase runners (Promise-based)
// ---------------------------------------------------------------------------

/**
 * Open the EventStore, write events up to each WRITE_MILESTONES boundary and
 * collect memory samples.  Returns a Promise that resolves with the samples
 * array once the store has been closed (which flushes all pending buffers).
 *
 * @param {string} dataDir
 * @returns {Promise<Array>}
 */
function runWritePhase(dataDir) {
    return new Promise((resolve, reject) => {
        const samples = [sample('Before EventStore init')];
        const store   = new EventStore('memory-test', { storageDirectory: dataDir });

        store.on('ready', () => {
            samples.push(sample('After ready (write baseline)'));

            let totalWritten = 0;

            for (const milestone of WRITE_MILESTONES) {
                while (totalWritten < milestone) {
                    const size   = Math.min(BATCH_SIZE, milestone - totalWritten);
                    const events = [];
                    for (let i = 0; i < size; i++) {
                        events.push({ type: 'MemTestEvent', seq: totalWritten + i });
                    }
                    // No callback – fire-and-forget.  The in-memory index is
                    // updated synchronously so the samples reflect index growth.
                    store.commit(STREAM_NAME, events);
                    totalWritten += size;
                }
                samples.push(sample(`After ${milestone} events written`));
            }

            // close() flushes all write buffers to disk so the read phase can
            // open the same store in read-only mode.
            store.close();
            resolve(samples);
        });

        store.on('error', reject);
    });
}

/**
 * Open the EventStore in read-only mode, iterate all events and collect
 * memory samples.  Returns a Promise that resolves with { samples, count }.
 *
 * @param {string} dataDir
 * @returns {Promise<{ samples: Array, count: number }>}
 */
function runReadPhase(dataDir) {
    return new Promise((resolve, reject) => {
        const samples = [sample('Before EventStore init')];
        const store   = new EventStore('memory-test', {
            storageDirectory: dataDir,
            readOnly: true,
        });

        store.on('ready', () => {
            samples.push(sample('After ready (read baseline)'));

            let count = 0;
            for (const _event of store.getEventStream(STREAM_NAME)) {
                count++;
            }

            samples.push(sample(`After reading ${count} events`));

            store.close();
            resolve({ samples, count });
        });

        store.on('error', reject);
    });
}

/**
 * Run an async scenario for LEAK_CYCLES iterations.
 * fn(index, next) is called once per cycle; call next(err) to advance.
 * Returns a Promise that resolves with { before, after } memory samples.
 *
 * @param {function(number, function)} fn
 * @returns {Promise<{ before: object, after: object }>}
 */
function runAsyncCycles(fn) {
    return new Promise((resolve, reject) => {
        const before = sample('');
        let i = 0;
        function next(err) {
            if (err) return reject(err);
            if (i >= LEAK_CYCLES) return resolve({ before, after: sample('') });
            fn(i++, next);
        }
        next(null);
    });
}

// ---------------------------------------------------------------------------
// Assertions
// ---------------------------------------------------------------------------

/**
 * Assert that RSS growth is not super-linear across the write milestones.
 * Throws an Error if the check fails.
 *
 * @param {Array} samples
 */
function assertNotSuperLinearGrowth(samples) {
    const baseline = samples.find(s => s.label === 'After ready (write baseline)');
    const low      = samples.find(s => s.label === `After ${WRITE_MILESTONES[0]} events written`);
    const high     = samples.find(s => s.label === `After ${WRITE_MILESTONES[WRITE_MILESTONES.length - 1]} events written`);

    if (!baseline || !low || !high) {
        console.warn('[memory] WARNING: could not locate all milestone samples for growth check.');
        return;
    }

    const growthLow  = low.rss  - baseline.rss;
    const growthHigh = high.rss - baseline.rss;

    const eventRatio   = WRITE_MILESTONES[WRITE_MILESTONES.length - 1] / WRITE_MILESTONES[0];
    const expectedSize = WRITE_MILESTONES[WRITE_MILESTONES.length - 1] * INDEX_ENTRY_BYTES;

    console.log('\nGrowth analysis (RSS, relative to write baseline):');
    console.log(`  After ${WRITE_MILESTONES[0].toLocaleString()} events : ${fmtMB(growthLow).trim()} MiB`);
    console.log(`  After ${WRITE_MILESTONES[WRITE_MILESTONES.length - 1].toLocaleString()} events: ${fmtMB(growthHigh).trim()} MiB`);
    console.log(`  Event count ratio        : ${eventRatio}×`);
    console.log(`  Index-only expected size : ${(expectedSize / 1024).toFixed(1)} KiB`);

    if (growthLow <= 0) {
        // The OS/GC absorbed the allocation – that is fine, skip ratio check.
        console.log('  RSS growth at first milestone is ≤ 0 (memory absorbed by OS/GC) — skipping ratio check.');
        return;
    }

    const ratio = growthHigh / growthLow;
    console.log(`  Actual RSS growth ratio  : ${ratio.toFixed(1)}× (limit: ${GROWTH_RATIO_LIMIT}×)`);

    if (ratio > GROWTH_RATIO_LIMIT) {
        throw new Error(
            `Memory growth is super-linear: RSS grew ${ratio.toFixed(1)}× for a ${eventRatio}× ` +
            `increase in events (limit: ${GROWTH_RATIO_LIMIT}×).`
        );
    }

    console.log(`  -> Growth is within acceptable bounds (not super-linear).`);
}

/**
 * Assert that heap growth per cycle is within the acceptable limit.
 * Throws an Error if the check fails.
 *
 * @param {string} label
 * @param {object} before
 * @param {object} after
 */
function assertNoLeak(label, before, after) {
    const rssGrowth  = after.rss      - before.rss;
    const heapGrowth = after.heapUsed - before.heapUsed;
    const perCycle   = heapGrowth / LEAK_CYCLES;
    const mbRss  = (rssGrowth  / 1024 / 1024).toFixed(2);
    const mbHeap = (heapGrowth / 1024 / 1024).toFixed(2);
    const kbPer  = (perCycle   / 1024).toFixed(2);
    const COL = 50;
    console.log(
        `  ${label.padEnd(COL)} heap: ${mbHeap.padStart(7)} MiB  rss: ${mbRss.padStart(7)} MiB  per-cycle: ${kbPer.padStart(8)} KiB`
    );

    if (perCycle > LEAK_BYTES_PER_CYCLE_LIMIT) {
        throw new Error(
            `Memory leak detected in "${label}": heap grew ${mbHeap} MiB over ${LEAK_CYCLES} cycles ` +
            `(${kbPer} KiB/cycle, limit: ${LEAK_BYTES_PER_CYCLE_LIMIT / 1024} KiB/cycle).`
        );
    }
}

// ---------------------------------------------------------------------------
// Mocha spec
// ---------------------------------------------------------------------------

describe('Memory stress test', function () {
    this.timeout(120_000);

    let dataDir;
    let writeSamples;

    before(function () {
        dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'es-memory-'));
    });

    after(function () {
        try { fs.rmSync(dataDir, { recursive: true, force: true }); } catch (_) { /* best-effort */ }
    });

    it('Phase 1 – write: memory growth is not super-linear', async function () {
        writeSamples = await runWritePhase(dataDir);
        printTable('Phase 1 – Write', writeSamples);
        assertNotSuperLinearGrowth(writeSamples);
    });

    it('Phase 2 – read: all events are readable', async function () {
        const { samples, count } = await runReadPhase(dataDir);
        printTable('Phase 2 – Read', samples);

        console.log(`\n  Total events read back : ${count.toLocaleString()}`);

        const expected = WRITE_MILESTONES[WRITE_MILESTONES.length - 1];
        if (count !== expected) {
            throw new Error(`Expected to read ${expected} events but got ${count}.`);
        }
    });

    it('Phase 3 – Index open/close: no memory leak', function () {
        const STREAMS_DIR = path.join(dataDir, 'streams');
        const INDEX_FILE  = 'memory-test.stream-' + STREAM_NAME + '.index';

        const before = sample('');
        for (let i = 0; i < LEAK_CYCLES; i++) {
            const idx = new Index.ReadOnly(INDEX_FILE, { dataDirectory: STREAMS_DIR });
            idx.close();
        }
        assertNoLeak('Index open/close', before, sample(''));
    });

    it('Phase 3 – Partition open/close: no memory leak', function () {
        const PARTITION_FILE = 'memory-test.' + STREAM_NAME;

        const before = sample('');
        for (let i = 0; i < LEAK_CYCLES; i++) {
            const part = new Partition.ReadOnly(PARTITION_FILE, { dataDirectory: dataDir });
            part.open();
            part.close();
        }
        assertNoLeak('Partition open/close', before, sample(''));
    });

    it('Phase 3 – Stream closeEventStream: no memory leak', async function () {
        this.timeout(60_000);
        const { before, after } = await runAsyncCycles((i, done) => {
            const cycleDir = fs.mkdtempSync(path.join(os.tmpdir(), 'es-strm-lk-'));
            const store    = new EventStore('s', { storageDirectory: cycleDir });
            store.on('ready', () => {
                store.commit('leak-stream', [{ type: 'LeakTest', i }], () => {
                    store.closeEventStream('leak-stream');
                    store.close();
                    try { fs.rmSync(cycleDir, { recursive: true, force: true }); } catch (_) { /* best-effort */ }
                    done(null);
                });
            });
            store.on('error', done);
        });
        assertNoLeak('Stream closeEventStream (make read-only)', before, after);
    });

    it('Phase 3 – EventStore open/close (writable): no memory leak', async function () {
        this.timeout(60_000);
        const { before, after } = await runAsyncCycles((i, done) => {
            const store = new EventStore('memory-test', { storageDirectory: dataDir });
            store.on('ready', () => { store.close(); done(null); });
            store.on('error', done);
        });
        assertNoLeak('EventStore open/close (writable)', before, after);
    });

    it('Phase 3 – EventStore open/close (read-only): no memory leak', async function () {
        this.timeout(60_000);
        const { before, after } = await runAsyncCycles((i, done) => {
            const store = new EventStore('memory-test', {
                storageDirectory: dataDir,
                readOnly: true,
            });
            store.on('ready', () => { store.close(); done(null); });
            store.on('error', done);
        });
        assertNoLeak('EventStore open/close (read-only)', before, after);
    });
});
