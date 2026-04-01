/**
 * Stress-test recovery script.
 *
 * Reopens the EventStore that was left in an unclean state (because the writer
 * was killed with SIGKILL) using the LOCK_RECLAIM option.  It then:
 *
 *  1. Verifies the store is readable after recovery.
 *  2. Writes new events to every stream to confirm the store is still writable.
 *  3. Computes how many events were lost in the simulated crash.
 *  4. Checks that the data loss is bounded by the write-buffer configuration
 *     that was used by the writer.
 *  5. Reports the results and exits with code 0 on success, 1 on failure.
 *
 * Usage:
 *   node recovery.js <dataDir> <statsFile>
 *
 *   dataDir   – same directory passed to writer.js
 *   statsFile – JSON file written by writer.js
 */

'use strict';

import path from 'path';
import fs from 'fs';

import EventStore, { LOCK_RECLAIM } from '../index.js';

// ---------------------------------------------------------------------------
// CLI args
// ---------------------------------------------------------------------------
const dataDir   = path.resolve(process.argv[2] || './data');
const statsFile = path.resolve(process.argv[3] || './writer-stats.json');

// ---------------------------------------------------------------------------
// Load writer stats
// ---------------------------------------------------------------------------
if (!fs.existsSync(statsFile)) {
    console.error('[recovery] Stats file not found:', statsFile);
    process.exit(1);
}

const stats = JSON.parse(fs.readFileSync(statsFile, 'utf8'));
console.log('[recovery] Writer stats loaded:');
console.log(`  Total written before crash : ${stats.totalWritten}`);
console.log(`  writeBufferSize            : ${stats.writeBufferSize}`);
console.log(`  maxWriteBufferDocuments    : ${stats.maxWriteBufferDocuments}`);

// ---------------------------------------------------------------------------
// Open the store with LOCK_RECLAIM so torn writes are repaired automatically
// ---------------------------------------------------------------------------
console.log('\n[recovery] Opening store with LOCK_RECLAIM ...');

const store = new EventStore('stress', {
    storageDirectory: dataDir,
    storageConfig: {
        lock: LOCK_RECLAIM,
    },
});

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
let failed = false;

function fail(msg) {
    console.error('\n[recovery] FAILURE:', msg);
    failed = true;
}

// ---------------------------------------------------------------------------
// Main recovery checks (run once the store emits 'ready')
// ---------------------------------------------------------------------------
store.on('ready', () => {
    console.log('[recovery] Store ready after LOCK_RECLAIM.');

    const totalAfterRecovery = store.length;
    console.log(`\n[recovery] Events in store after recovery: ${totalAfterRecovery}`);

    // -----------------------------------------------------------------------
    // 1. Verify the store is readable: iterate every known stream
    // -----------------------------------------------------------------------
    console.log('\n[recovery] Checking readability of all streams ...');
    const streamNames = Object.keys(stats.writtenPerStream);
    const readCounts  = {};

    for (const streamName of streamNames) {
        let count = 0;
        try {
            for (const _event of store.getEventStream(streamName)) {
                count++;
            }
        } catch (err) {
            fail(`Could not read stream "${streamName}": ${err.message}`);
        }
        readCounts[streamName] = count;
        console.log(`  ${streamName}: ${count} events readable`);
    }

    // -----------------------------------------------------------------------
    // 2. Write new events to every stream to confirm writability
    // -----------------------------------------------------------------------
    console.log('\n[recovery] Writing one new event to each stream ...');
    let pendingWrites = streamNames.length;

    function onWriteDone(streamName) {
        console.log(`  Wrote verification event to "${streamName}"`);
        pendingWrites--;
        if (pendingWrites === 0) {
            afterAllWrites();
        }
    }

    for (const streamName of streamNames) {
        store.commit(
            streamName,
            [{ type: 'RecoveryVerification', ts: Date.now() }],
            () => onWriteDone(streamName)
        );
    }
});

function afterAllWrites() {
    console.log('[recovery] All verification writes succeeded.');

    // -----------------------------------------------------------------------
    // 3. Compute data loss
    // -----------------------------------------------------------------------
    const totalAfterRecovery = store.length - Object.keys(stats.writtenPerStream).length; // subtract the verification events
    const lostEvents         = stats.totalWritten - totalAfterRecovery;

    console.log('\n[recovery] Data loss summary:');
    console.log(`  Events written before crash : ${stats.totalWritten}`);
    console.log(`  Events in store after repair: ${totalAfterRecovery}`);
    console.log(`  Events lost in crash        : ${lostEvents}`);

    if (lostEvents < 0) {
        fail(`Negative data loss (${lostEvents}) – store has MORE events than writer reported. Possible stats corruption.`);
    }

    // -----------------------------------------------------------------------
    // 4. Verify that data loss is bounded
    //
    // With index auto-repair (reindex() is invoked automatically during
    // checkTornWrites() when the primary index is found to be lagging), events
    // whose partition data reached disk but whose index entries did not are
    // recovered automatically.  Data loss therefore comes only from:
    //
    //   a) Partition write buffer – controlled by maxWriteBufferDocuments and
    //      writeBufferSize.  With maxWriteBufferDocuments > 0 each partition
    //      buffers at most that many documents before flushing.  Documents
    //      still in the buffer at crash time are never written to disk and
    //      cannot be recovered.
    //
    //   b) One torn (partially-written) document per partition at the moment
    //      of the crash.  The torn document is truncated during recovery, so
    //      at most one commit's worth of events may be lost this way.
    //
    // The hard upper bound on total data loss is therefore:
    //
    //   partitionBufferBound  (from partition write-buffer docs, per stream)
    //   + maxBatchSize  (one in-flight commit whose last events may be torn)
    //
    // -----------------------------------------------------------------------
    const { writeBufferSize, maxWriteBufferDocuments, numStreams, maxBatchSize } = stats;

    // Each partition buffers at most maxWriteBufferDocuments docs; if not set,
    // fall back to a byte-based estimate (100 bytes is a conservative min size).
    const perPartitionBound    = maxWriteBufferDocuments > 0
        ? maxWriteBufferDocuments
        : Math.ceil(writeBufferSize / 100);
    const partitionBufferBound = perPartitionBound * numStreams;

    const allowedLoss = partitionBufferBound + maxBatchSize;

    console.log(`\n[recovery] Data-loss bound check:`);
    console.log(`  Partition buffer bound      : ${partitionBufferBound} events (${perPartitionBound} per stream × ${numStreams} streams)`);
    console.log(`  Torn-commit allowance       : ${maxBatchSize} events`);
    console.log(`  Total allowed loss          : ${allowedLoss} events`);
    console.log(`  Actual loss                 : ${lostEvents} events`);

    if (lostEvents > allowedLoss) {
        fail(
            `Data loss (${lostEvents}) exceeds the expected upper bound (${allowedLoss}). ` +
            `This suggests events were lost beyond what the write buffer allows.`
        );
    }

    // -----------------------------------------------------------------------
    // 5. Clean close
    // -----------------------------------------------------------------------
    store.close();
    console.log('\n[recovery] Store closed cleanly.');

    if (failed) {
        console.error('\n[recovery] *** STRESS TEST FAILED ***');
        process.exit(1);
    }

    console.log('\n[recovery] *** STRESS TEST PASSED ***');
    process.exit(0);
}

store.on('error', (err) => {
    console.error('[recovery] Store error:', err);
    process.exit(1);
});
