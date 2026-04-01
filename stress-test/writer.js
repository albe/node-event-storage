/**
 * Stress-test writer script.
 *
 * Writes events in a continuous loop across several streams so that the parent
 * orchestration script can kill this process mid-write and test crash recovery.
 *
 * State persisted to disk (via STATS_FILE) is used by recovery.js to compute
 * how many events were lost in the simulated crash.
 *
 * Usage:
 *   node writer.js <dataDir> <statsFile>
 *
 *   dataDir   – directory where the EventStore keeps its data
 *   statsFile – JSON file that receives a snapshot of write counts after every
 *               successful commit so recovery.js can compare against it
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
// Configuration
// ---------------------------------------------------------------------------
const STREAM_NAMES   = ['orders', 'users', 'inventory', 'payments', 'notifications'];
const WRITE_BUFFER   = 4096;   // bytes – keep small to maximise crash-window visibility
const MAX_DOCS       = 5;      // flush after at most this many docs in the buffer
const MAX_BATCH      = 3;      // maximum events per commit (see buildEvents)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function randomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randomStream() {
    return STREAM_NAMES[randomInt(0, STREAM_NAMES.length - 1)];
}

function buildEvents(count) {
    const events = [];
    for (let i = 0; i < count; i++) {
        events.push({
            type: 'StressEvent',
            payload: {
                value: randomInt(1, 1_000_000),
                ts: Date.now(),
            },
        });
    }
    return events;
}

// ---------------------------------------------------------------------------
// Stats – persisted after every successful commit
// ---------------------------------------------------------------------------
const stats = {
    writtenPerStream: {},
    totalWritten: 0,
    writeBufferSize: WRITE_BUFFER,
    maxWriteBufferDocuments: MAX_DOCS,
    numStreams: STREAM_NAMES.length,
    maxBatchSize: MAX_BATCH,
};

for (const s of STREAM_NAMES) {
    stats.writtenPerStream[s] = 0;
}

function persistStats() {
    // Write to a temporary file first then rename so that a SIGKILL mid-write
    // cannot leave the stats file in a partially-written / corrupt state.
    const tmp = statsFile + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(stats, null, 2));
    fs.renameSync(tmp, statsFile);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
const store = new EventStore('stress', {
    storageDirectory: dataDir,
    storageConfig: {
        writeBufferSize: WRITE_BUFFER,
        maxWriteBufferDocuments: MAX_DOCS,
    },
});

store.on('ready', () => {
    console.log('[writer] EventStore ready. Starting write loop ...');
    console.log(`[writer] Streams: ${STREAM_NAMES.join(', ')}`);
    console.log(`[writer] writeBufferSize=${WRITE_BUFFER} maxWriteBufferDocuments=${MAX_DOCS}`);

    function writeNext() {
        const stream     = randomStream();
        const batchSize  = randomInt(1, MAX_BATCH);
        const events     = buildEvents(batchSize);

        store.commit(stream, events, () => {
            stats.writtenPerStream[stream] += batchSize;
            stats.totalWritten            += batchSize;
            persistStats();

            // Tiny random pause (0–2 ms) to vary timing across commits
            const delay = randomInt(0, 2);
            setTimeout(writeNext, delay);
        });
    }

    writeNext();
});

store.on('error', (err) => {
    console.error('[writer] Store error:', err);
    process.exit(1);
});
