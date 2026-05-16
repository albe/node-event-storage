import fs from 'fs/promises';
import os from 'os';
import path from 'path';
import { once } from 'events';
import { performance } from 'perf_hooks';
import EventStore from '../../index.js';
import ReadablePartition from '../../src/Partition/ReadablePartition.js';
import AsyncReadablePartition from '../src/AsyncReadablePartition.js';

async function createFixture(storageDirectory, eventCount = 4000) {
    const store = new EventStore({
        storageDirectory
    });
    await once(store, 'ready');

    const events = Array.from({ length: eventCount }, (_, index) => ({
        type: 'Benchmarked',
        index,
        payload: `event-${index}`.repeat(2)
    }));
    await new Promise((resolve, reject) => {
        try {
            store.commit('bench-stream', events, resolve);
        } catch (error) {
            reject(error);
        }
    });

    const partitionPath = store.storage.getPartition('bench-stream').fileName;
    store.close();
    return {
        dataDirectory: storageDirectory,
        partitionName: path.basename(partitionPath)
    };
}

function runSyncReaders({ dataDirectory, partitionName }, concurrency, passes) {
    const startedAt = performance.now();
    let bytesRead = 0;
    for (let readerIndex = 0; readerIndex < concurrency; readerIndex++) {
        const partition = new ReadablePartition(partitionName, { dataDirectory });
        partition.open();
        try {
            for (let pass = 0; pass < passes; pass++) {
                for (const document of partition.readAll()) {
                    bytesRead += document.length;
                }
            }
        } finally {
            partition.close();
        }
    }
    return {
        bytesRead,
        durationMs: performance.now() - startedAt
    };
}

async function runAsyncReaders({ dataDirectory, partitionName }, concurrency, passes) {
    const startedAt = performance.now();
    const readerTotals = await Promise.all(
        Array.from({ length: concurrency }, async () => {
            const partition = new AsyncReadablePartition(partitionName, { dataDirectory });
            await partition.open();
            let bytesRead = 0;
            try {
                for (let pass = 0; pass < passes; pass++) {
                    for await (const document of partition.readAll()) {
                        bytesRead += document.length;
                    }
                }
            } finally {
                await partition.close();
            }
            return bytesRead;
        })
    );

    return {
        bytesRead: readerTotals.reduce((sum, value) => sum + value, 0),
        durationMs: performance.now() - startedAt
    };
}

async function main() {
    const concurrency = Number(process.env.BENCH_CONCURRENCY || 8);
    const passes = Number(process.env.BENCH_PASSES || 4);
    const storageDirectory = await fs.mkdtemp(path.join(os.tmpdir(), 'event-storage-http-bench-'));

    try {
        const fixture = await createFixture(storageDirectory);
        const syncResult = runSyncReaders(fixture, concurrency, passes);
        const asyncResult = await runAsyncReaders(fixture, concurrency, passes);

        console.log(JSON.stringify({
            concurrency,
            passes,
            sync: syncResult,
            async: asyncResult,
            ratio: Number((syncResult.durationMs / asyncResult.durationMs).toFixed(3))
        }, null, 2));
    } finally {
        await fs.rm(storageDirectory, { recursive: true, force: true });
    }
}

main().catch(error => {
    console.error(error);
    process.exitCode = 1;
});
