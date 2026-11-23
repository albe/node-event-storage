/*
 * Write bursts with group commit (`syncInterval`).
 *
 *   node bench/bench-durability.js [fsyncLatencyMs] [syncInterval]
 *
 * fsyncLatencyMs simulates a slower disk by adding that latency to every fsync (e.g. 5 for a cloud persistent disk).
 * Reports throughput, the number of fsyncs and the commit latency until the callback fired.
 */
import fs from 'fs';
import path from 'path';
import os from 'os';
import { EventStore } from '../index.js';
import { simulateFsyncLatency, stats, reset } from './fsyncLatency.js';

const FSYNC_LATENCY = Number(process.argv[2] ?? 5);
const SYNC_INTERVAL = Number(process.argv[3] ?? 20);

await simulateFsyncLatency(FSYNC_LATENCY);

function openStore() {
    const storageDirectory = fs.mkdtempSync(path.join(os.tmpdir(), 'bench-durability-'));
    return new Promise(resolve => {
        const store = new EventStore('bench', { storageDirectory, storageConfig: { syncInterval: SYNC_INTERVAL } });
        store.on('ready', () => resolve({ store, storageDirectory }));
    });
}

const event = { type: 'SomethingHappened', doc: 'this is some test document for measuring performance', value: 123.45 };

async function burst(name, events, streamOf) {
    const { store, storageDirectory } = await openStore();
    reset();
    const start = performance.now();
    await new Promise(resolve => {
        let pending = events;
        for (let i = 0; i < events; i++) {
            store.commit(streamOf(i), event, () => --pending === 0 && resolve());
        }
    });
    report(name, events, performance.now() - start);
    await new Promise(resolve => store.close(resolve));
    fs.rmSync(storageDirectory, { recursive: true, force: true });
}

async function sequential(name, events, streamOf) {
    const { store, storageDirectory } = await openStore();
    reset();
    const start = performance.now();
    for (let i = 0; i < events; i++) {
        await new Promise(resolve => store.commit(streamOf(i), event, resolve));
    }
    report(name, events, performance.now() - start);
    await new Promise(resolve => store.close(resolve));
    fs.rmSync(storageDirectory, { recursive: true, force: true });
}

function report(name, events, ms) {
    console.log(`${name.padEnd(44)} ${String(Math.round(events / ms * 1000)).padStart(8)} events/s  ${(stats.fsyncs / events).toFixed(2).padStart(6)} fsyncs/event  ${(ms / events).toFixed(2).padStart(7)} ms/event`);
}

console.log(`fsync latency ${FSYNC_LATENCY} ms, syncInterval ${SYNC_INTERVAL} ms`);
await burst('burst: 20000 events, 1 stream', 20000, () => 'stream');
await burst('burst: 2000 events, 100 streams', 2000, i => 'stream-' + (i % 100));
await burst('burst: 2000 events, 2000 new streams', 2000, i => 'stream-' + i);
await sequential('sequential: 50 commits, 1 stream', 50, () => 'stream');
await sequential('sequential: 50 commits, 50 new streams', 50, i => 'stream-' + i);
