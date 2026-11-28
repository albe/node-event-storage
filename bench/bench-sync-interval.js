/*
 * Commit latency, throughput and fsync load for different `syncInterval` values.
 *
 *   node bench/bench-sync-interval.js [fsyncLatencyMs] [writers] [inflight] [seconds] [--json]
 *
 * Each of `writers` concurrent writers commits to its own stream. A writer keeps `inflight` commits pending and
 * commits again as soon as one of them is acknowledged. With `inflight` 1 it awaits every commit, like an import
 * that awaits each commit, so every commit waits for a full sync run. A higher `inflight` pipelines the commits.
 * fsyncLatencyMs adds that latency to every fsync to simulate a slower disk (e.g. 2 for a cloud persistent disk).
 *
 * Reports per syncInterval: events/s, median and p99 commit latency, fsyncs/s, the share of time the process was
 * blocked in fsync (the event loop does nothing else meanwhile, the CPU is idle waiting for the disk) and the
 * share of time the process used the CPU.
 */
import fs from 'fs';
import path from 'path';
import os from 'os';
import { EventStore } from '../index.js';
import { simulateFsyncLatency, stats, reset } from './fsyncLatency.js';

const args = process.argv.slice(2).filter(arg => !arg.startsWith('--'));
const FSYNC_LATENCY = Number(args[0] ?? 0);
const WRITERS = Number(args[1] ?? 1);
const INFLIGHT = Number(args[2] ?? 1);
const SECONDS = Number(args[3] ?? 3);
const JSON_OUTPUT = process.argv.includes('--json');
const INTERVALS = [1, 2, 5, 10, 20, 50, 100, 200];

await simulateFsyncLatency(FSYNC_LATENCY);

const event = { type: 'SomethingHappened', doc: 'this is some test document for measuring performance', value: 123.45 };

function openStore(storageConfig) {
    const storageDirectory = fs.mkdtempSync(path.join(os.tmpdir(), 'bench-sync-interval-'));
    return new Promise(resolve => {
        const store = new EventStore('bench', { storageDirectory, storageConfig });
        store.on('ready', () => resolve({ store, storageDirectory }));
    });
}

function commit(store, stream) {
    return new Promise(resolve => store.commit(stream, event, resolve));
}

async function measure(label, storageConfig) {
    const { store, storageDirectory } = await openStore(storageConfig);
    const streams = Array.from({ length: WRITERS }, (_, i) => 'stream-' + i);
    // Create the streams first, so that only commits to existing streams are measured.
    await Promise.all(streams.map(stream => commit(store, stream)));

    const latencies = [];
    reset();
    const cpu = process.cpuUsage();
    const start = performance.now();
    const end = start + SECONDS * 1000;
    const writer = async stream => {
        while (performance.now() < end) {
            const begin = performance.now();
            await commit(store, stream);
            latencies.push(performance.now() - begin);
        }
    };
    await Promise.all(streams.flatMap(stream => Array.from({ length: INFLIGHT }, () => writer(stream))));
    const wall = performance.now() - start;
    const { user, system } = process.cpuUsage(cpu);

    store.close();
    fs.rmSync(storageDirectory, { recursive: true, force: true });

    latencies.sort((a, b) => a - b);
    const events = latencies.length;
    return {
        label,
        syncInterval: storageConfig.syncInterval ?? 0,
        eventsPerSecond: events / wall * 1000,
        medianLatency: latencies[Math.floor(events / 2)],
        p99Latency: latencies[Math.floor(events * 0.99)],
        fsyncsPerSecond: stats.fsyncs / wall * 1000,
        fsyncBlocked: stats.blockedTime / wall,
        cpu: (user + system) / 1000 / wall,
    };
}

const results = [];
results.push(await measure('no sync', {}));
for (const syncInterval of INTERVALS) {
    results.push(await measure(`syncInterval ${syncInterval}`, { syncInterval }));
}
results.push(await measure('syncOnFlush', { syncOnFlush: true }));

if (JSON_OUTPUT) {
    console.log(JSON.stringify({ fsyncLatency: FSYNC_LATENCY, writers: WRITERS, inflight: INFLIGHT, seconds: SECONDS, results }, null, 2));
} else {
    console.log(`fsync latency +${FSYNC_LATENCY} ms, ${WRITERS} writer(s) with ${INFLIGHT} commit(s) in flight, ${SECONDS} s each`);
    console.log('config'.padEnd(18) + 'events/s'.padStart(10) + 'median ms'.padStart(11) + 'p99 ms'.padStart(9)
        + 'fsyncs/s'.padStart(10) + 'in fsync'.padStart(10) + 'CPU'.padStart(9));
    for (const r of results) {
        console.log(r.label.padEnd(18)
            + Math.round(r.eventsPerSecond).toString().padStart(10)
            + r.medianLatency.toFixed(2).padStart(11)
            + r.p99Latency.toFixed(2).padStart(9)
            + Math.round(r.fsyncsPerSecond).toString().padStart(10)
            + ((r.fsyncBlocked * 100).toFixed(1) + ' %').padStart(10)
            + ((r.cpu * 100).toFixed(1) + ' %').padStart(9));
    }
}
