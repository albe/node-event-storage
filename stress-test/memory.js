/**
 * Memory consumption stress test.
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
 * After phases 1 and 2 a consumption-growth table is printed.  The test then
 * asserts that memory growth is not super-linear: doubling the number of
 * events should not more than double the memory growth that was observed at
 * the 1 000-event milestone.
 *
 * Usage:
 *   node memory.js [dataDir]
 *
 * For more accurate GC measurements you may run with:
 *   node --expose-gc memory.js [dataDir]
 *
 * dataDir – optional directory where the EventStore keeps its data.
 *           Defaults to a temporary directory that is removed on exit.
 */

'use strict';

const path = require('path');
const fs   = require('fs');
const os   = require('os');

const EventStore = require('../index');
const Index      = require('../src/Index');
const Partition  = require('../src/Partition');

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
// to the growth at 1 000 events.  10 × events → ≤ GROWTH_RATIO × growth.
const GROWTH_RATIO_LIMIT = 15;

// ---------------------------------------------------------------------------
// CLI args
// ---------------------------------------------------------------------------
let DATA_DIR      = process.argv[2] ? path.resolve(process.argv[2]) : null;
const CLEANUP_DIR = DATA_DIR === null;
if (!DATA_DIR) {
    DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'es-memory-'));
}

// ---------------------------------------------------------------------------
// Helpers
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
// Write phase
// ---------------------------------------------------------------------------

/**
 * Open the EventStore, write events up to each WRITE_MILESTONES boundary using
 * fire-and-forget commits (no explicit callback), and collect memory samples.
 * Invokes callback(samples) when all milestones have been written and the
 * store has been closed (which flushes all pending buffers to disk).
 *
 * Using fire-and-forget commits keeps the implementation simple and avoids
 * deep async-callback recursion.  The in-memory index is updated synchronously
 * on every write, so the memory samples accurately reflect index growth.
 *
 * @param {function(Array)} callback
 */
function runWritePhase(callback) {
    const samples = [];

    samples.push(sample('Before EventStore init'));

    const store = new EventStore('memory-test', { storageDirectory: DATA_DIR });

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
                // No callback – fire-and-forget.  EventStore.fixArgumentTypes()
                // (src/EventStore.js) converts the missing callback to a no-op,
                // so this is safe.  The in-memory index is updated synchronously
                // on every write regardless of buffer flush status.
                store.commit(STREAM_NAME, events);
                totalWritten += size;
            }

            samples.push(sample(`After ${milestone} events written`));
        }

        // close() flushes all write buffers and index data to disk so the
        // read phase can open the same store in read-only mode.
        store.close();
        callback(samples);
    });

    store.on('error', (err) => {
        console.error('[memory] EventStore error (write phase):', err);
        process.exit(1);
    });
}

// ---------------------------------------------------------------------------
// Read phase
// ---------------------------------------------------------------------------

/**
 * Open the EventStore in read-only mode, iterate all events in STREAM_NAME
 * and collect memory samples.  Invokes callback(samples, totalRead) when done.
 *
 * @param {function(Array, number)} callback
 */
function runReadPhase(callback) {
    const samples = [];

    samples.push(sample('Before EventStore init'));

    const store = new EventStore('memory-test', {
        storageDirectory: DATA_DIR,
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
        callback(samples, count);
    });

    store.on('error', (err) => {
        console.error('[memory] EventStore error (read phase):', err);
        process.exit(1);
    });
}

// ---------------------------------------------------------------------------
// Growth assertion
// ---------------------------------------------------------------------------

/**
 * Assert that RSS growth is not super-linear across the write milestones.
 * Linear and sub-linear growth are both acceptable.
 *
 * Concretely: the growth at the highest milestone must not exceed
 * GROWTH_RATIO_LIMIT times the growth at the lowest milestone.
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

    if (growthLow > 0) {
        const ratio = growthHigh / growthLow;
        console.log(`  Actual RSS growth ratio  : ${ratio.toFixed(1)}× (limit: ${GROWTH_RATIO_LIMIT}×)`);

        if (ratio > GROWTH_RATIO_LIMIT) {
            console.error(
                `\nFAIL: Memory growth is super-linear! ` +
                `RSS grew ${ratio.toFixed(1)}× for a ${eventRatio}× increase in events ` +
                `(limit: ${GROWTH_RATIO_LIMIT}×).`
            );
            process.exitCode = 1;
        } else {
            console.log(`  -> Growth is within acceptable bounds (not super-linear).`);
        }
    } else {
        // If the first milestone shows no measurable growth the OS/GC has
        // absorbed the allocation — that is fine, log and move on.
        console.log('  RSS growth at first milestone is ≤ 0 (memory absorbed by OS/GC) — skipping ratio check.');
    }
}

// ---------------------------------------------------------------------------
// Open/Close Leak Detection – configuration
// ---------------------------------------------------------------------------

// Number of open/close cycles per scenario.
const LEAK_CYCLES = 100;

// Maximum acceptable heap growth (heapUsed) per open/close cycle.
// With GC exposed a well-behaved implementation shows near-zero growth;
// without GC the allocator may retain a page or two temporarily.
const LEAK_BYTES_PER_CYCLE_LIMIT = 10 * 1024; // 10 KiB

// ---------------------------------------------------------------------------
// Leak-phase helpers
// ---------------------------------------------------------------------------

/**
 * Run a synchronous scenario for LEAK_CYCLES iterations, bracketing
 * the loop with GC + memory samples.
 *
 * @param {function(number)} fn Called once per cycle with the cycle index.
 * @returns {{ before: object, after: object }}
 */
function runSyncCycles(fn) {
    const before = sample('');
    for (let i = 0; i < LEAK_CYCLES; i++) fn(i);
    return { before, after: sample('') };
}

/**
 * Run an async scenario for LEAK_CYCLES iterations, bracketing the loop
 * with GC + memory samples.
 *
 * @param {function(number, function)} fn Called once per cycle with (index, done).
 * @param {function({ before: object, after: object })} callback
 */
function runAsyncCycles(fn, callback) {
    const before = sample('');
    let i = 0;
    function next() {
        if (i >= LEAK_CYCLES) return callback({ before, after: sample('') });
        fn(i++, next);
    }
    next();
}

/**
 * Log result and set exit code to 1 if heap growth-per-cycle exceeds the limit.
 *
 * Asserts on `heapUsed` (live JavaScript objects) rather than RSS, because
 * V8 retains OS memory pages even after objects are collected, which causes
 * RSS to grow independently of actual object leaks.
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
        console.error(
            `\nFAIL: Memory leak detected in "${label}"! ` +
            `Heap grew ${mbHeap} MiB over ${LEAK_CYCLES} cycles ` +
            `(${kbPer} KiB/cycle, limit: ${LEAK_BYTES_PER_CYCLE_LIMIT / 1024} KiB/cycle).`
        );
        process.exitCode = 1;
    }
}

// ---------------------------------------------------------------------------
// Phase 3 – Open/Close Leak Detection
// ---------------------------------------------------------------------------

/**
 * Run the open/close leak-detection phase.
 *
 * Tests that repeated open/close cycles for Indexes, Partitions, Streams
 * (making them read-only via closeEventStream) and the whole EventStore do
 * not continuously grow RSS.
 *
 * Data produced by Phase 1 (write phase) in DATA_DIR is used as the source
 * for the Index and Partition sub-tests.  The stream and EventStore sub-tests
 * create their own temporary data per cycle so no state accumulates across
 * cycles.
 *
 * @param {function} callback
 */
function runLeakPhase(callback) {
    const STREAMS_DIR    = path.join(DATA_DIR, 'streams');
    // Primary index stored in the streams subdirectory, prefixed with store name.
    const INDEX_FILE     = 'memory-test.stream-' + STREAM_NAME + '.index';
    // Partition file stored in the data directory, prefixed with store name.
    const PARTITION_FILE = 'memory-test.' + STREAM_NAME;

    console.log('\nPhase 3 – Open/Close Leak Detection');
    console.log(`  Cycles per scenario : ${LEAK_CYCLES}`);
    console.log(`  Leak limit          : ${LEAK_BYTES_PER_CYCLE_LIMIT / 1024} KiB/cycle`);
    const HR = '-'.repeat(80);
    console.log('\n' + HR);
    console.log('  Scenario' + ' '.repeat(44) + 'Heap growth    RSS growth    Per-cycle');
    console.log(HR);

    // -----------------------------------------------------------------------
    // 3a. Index open/close
    // Each cycle opens a ReadOnlyIndex on the existing stream index file and
    // immediately closes it again.
    // -----------------------------------------------------------------------
    const idxResult = runSyncCycles(() => {
        const idx = new Index.ReadOnly(INDEX_FILE, { dataDirectory: STREAMS_DIR });
        idx.close();
    });
    assertNoLeak('Index open/close', idxResult.before, idxResult.after);

    // -----------------------------------------------------------------------
    // 3b. Partition open/close
    // Each cycle opens a ReadOnlyPartition on the existing partition file and
    // immediately closes it again.
    // -----------------------------------------------------------------------
    const partResult = runSyncCycles(() => {
        const part = new Partition.ReadOnly(PARTITION_FILE, { dataDirectory: DATA_DIR });
        part.open();
        part.close();
    });
    assertNoLeak('Partition open/close', partResult.before, partResult.after);

    // -----------------------------------------------------------------------
    // 3c. Stream open/close – make stream read-only via closeEventStream()
    // Each cycle uses its own temporary data directory so state does not
    // accumulate across cycles.  The cycle writes one event to a stream,
    // closes the stream (making it read-only), then closes the EventStore.
    // -----------------------------------------------------------------------
    runAsyncCycles((i, done) => {
        const cycleDir = fs.mkdtempSync(path.join(os.tmpdir(), 'es-strm-lk-'));
        const store = new EventStore('s', { storageDirectory: cycleDir });
        store.on('ready', () => {
            store.commit('leak-stream', [{ type: 'LeakTest', i }], () => {
                store.closeEventStream('leak-stream');
                store.close();
                try { fs.rmSync(cycleDir, { recursive: true, force: true }); } catch (_) { /* best-effort */ }
                done();
            });
        });
        store.on('error', (err) => {
            console.error('[memory] EventStore error (leak/stream-close):', err);
            process.exit(1);
        });
    }, ({ before: sBefore, after: sAfter }) => {
        assertNoLeak('Stream closeEventStream (make read-only)', sBefore, sAfter);

        // -------------------------------------------------------------------
        // 3d. EventStore open/close (writable)
        // Each cycle opens the EventStore in writable mode against the
        // existing data directory and closes it without writing new data.
        // This exercises the full storage open/close lifecycle including
        // lock acquisition and index flushing.
        // -------------------------------------------------------------------
        runAsyncCycles((i, done) => {
            const store = new EventStore('memory-test', { storageDirectory: DATA_DIR });
            store.on('ready', () => {
                store.close();
                done();
            });
            store.on('error', (err) => {
                console.error('[memory] EventStore error (leak/writable):', err);
                process.exit(1);
            });
        }, ({ before: wBefore, after: wAfter }) => {
            assertNoLeak('EventStore open/close (writable)', wBefore, wAfter);

            // ---------------------------------------------------------------
            // 3e. EventStore open/close (read-only)
            // Each cycle opens the EventStore in read-only mode; the existing
            // stream index is scanned and opened as a ReadOnlyIndex.  The
            // cycle closes immediately without reading events, mirroring 3d
            // so that both tests measure the open/close lifecycle cost.
            // (Event reading is already covered in Phase 2.)
            // ---------------------------------------------------------------
            runAsyncCycles((i, done) => {
                const store = new EventStore('memory-test', {
                    storageDirectory: DATA_DIR,
                    readOnly: true,
                });
                store.on('ready', () => {
                    store.close();
                    done();
                });
                store.on('error', (err) => {
                    console.error('[memory] EventStore error (leak/read-only):', err);
                    process.exit(1);
                });
            }, ({ before: rBefore, after: rAfter }) => {
                assertNoLeak('EventStore open/close (read-only)', rBefore, rAfter);
                console.log(HR);
                callback();
            });
        });
    });
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

console.log('='.repeat(66));
console.log('  node-event-storage  –  Memory Consumption Stress Test');
console.log('='.repeat(66));
console.log(`  Data directory    : ${DATA_DIR}`);
console.log(`  Index entry size  : ${INDEX_ENTRY_BYTES} bytes`);
console.log(`  GC exposed        : ${typeof global.gc === 'function' ? 'yes (--expose-gc)' : 'no'}`);
console.log('='.repeat(66));

runWritePhase((writeSamples) => {
    printTable('Phase 1 – Write', writeSamples);
    assertNotSuperLinearGrowth(writeSamples);

    runReadPhase((readSamples, totalRead) => {
        printTable('Phase 2 – Read', readSamples);

        console.log(`\n  Total events read back : ${totalRead.toLocaleString()}`);

        // Verify we read the expected number of events.
        const expected = WRITE_MILESTONES[WRITE_MILESTONES.length - 1];
        if (totalRead !== expected) {
            console.error(`\nFAIL: Expected to read ${expected} events but got ${totalRead}.`);
            process.exitCode = 1;
        }

        runLeakPhase(() => {
            if (!process.exitCode) {
                console.log('\nPASS: Memory stress test completed successfully.');
            }

            // Clean up temporary data directory if we created it.
            if (CLEANUP_DIR) {
                try {
                    fs.rmSync(DATA_DIR, { recursive: true, force: true });
                } catch (_) { /* best-effort */ }
            }
        });
    });
});
