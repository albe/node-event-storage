import fs from 'fs/promises';
import os from 'os';
import path from 'path';
import { once } from 'events';
import EventStore from '../../index.js';
import { DOCUMENT_HEADER_SIZE } from '../../src/Partition/ReadablePartition.js';

function toNumber(value, fallback) {
    const num = Number(value);
    return Number.isFinite(num) && num > 0 ? Math.floor(num) : fallback;
}

async function createFixture(storageDirectory, streamCount, eventsPerStream, payloadSize) {
    const store = new EventStore('bench', {
        storageDirectory
    });
    await once(store, 'ready');

    const streams = Array.from({ length: streamCount }, (_, i) => `stream-${i + 1}`);
    const payloadBase = 'x'.repeat(Math.max(8, payloadSize - 32));
    const totalEvents = streamCount * eventsPerStream;
    for (let i = 0; i < totalEvents; i++) {
        const stream = streams[i % streams.length];
        const seq = Math.floor(i / streams.length);
        store.commit(stream, {
            type: 'BenchmarkedEvent',
            seq,
            stream,
            payload: `${payloadBase}${seq.toString(36)}`
        });
    }

    const readStore = new EventStore('bench', {
        storageDirectory,
        readOnly: true
    });
    await once(readStore, 'ready');
    store.close();
    return readStore;
}

async function createPartitionData(readStore) {
    const partitions = {};
    for (const partition of readStore.storage.partitions.values()) {
        partition.open();
        partitions[partition.id] = {
            fileName: partition.fileName,
            headerSize: partition.headerSize
        };
    }

    const partitionBuffers = {};
    for (const [partitionId, info] of Object.entries(partitions)) {
        const buffer = await fs.readFile(info.fileName);
        partitionBuffers[partitionId] = buffer;
    }

    return { partitions, partitionBuffers };
}

function collectEntries(index, from = 1, until = -1) {
    const max = until < 0 ? index.length : until;
    if (max < from) {
        return [];
    }
    return index.range(from, max);
}

function benchmarkRawFullPartition(buffer, passes, chunkSize = 64 * 1024) {
    let bytes = 0;
    let checksum = 0;
    const startedAt = performance.now();
    for (let pass = 0; pass < passes; pass++) {
        for (let offset = 0; offset < buffer.byteLength; offset += chunkSize) {
            const end = Math.min(offset + chunkSize, buffer.byteLength);
            const chunk = buffer.subarray(offset, end);
            bytes += chunk.byteLength;
            checksum ^= chunk[0] || 0;
            checksum ^= chunk[chunk.byteLength - 1] || 0;
            checksum ^= chunk[(chunk.byteLength >> 1)] || 0;
        }
    }
    return {
        bytes,
        checksum,
        durationMs: performance.now() - startedAt
    };
}

function benchmarkNdjsonSectionsSingleFile(buffer, headerSize, entries, passes) {
    let bytes = 0;
    let checksum = 0;
    const startedAt = performance.now();
    for (let pass = 0; pass < passes; pass++) {
        for (const entry of entries) {
            const start = headerSize + entry.position + DOCUMENT_HEADER_SIZE;
            const end = start + entry.size;
            const slice = buffer.subarray(start, end);
            bytes += slice.byteLength + 1;
            checksum ^= slice[0] || 0;
            checksum ^= slice[slice.byteLength - 1] || 0;
            checksum ^= 10;
        }
    }
    return {
        bytes,
        checksum,
        durationMs: performance.now() - startedAt
    };
}

function benchmarkNdjsonSectionsMixedFiles(entries, partitionBuffers, partitions, passes) {
    let bytes = 0;
    let checksum = 0;
    const startedAt = performance.now();
    for (let pass = 0; pass < passes; pass++) {
        for (const entry of entries) {
            const buffer = partitionBuffers[entry.partition];
            const headerSize = partitions[entry.partition].headerSize;
            const start = headerSize + entry.position + DOCUMENT_HEADER_SIZE;
            const end = start + entry.size;
            const slice = buffer.subarray(start, end);
            bytes += slice.byteLength + 1;
            checksum ^= slice[0] || 0;
            checksum ^= slice[slice.byteLength - 1] || 0;
            checksum ^= 10;
        }
    }
    return {
        bytes,
        checksum,
        durationMs: performance.now() - startedAt
    };
}

function toMetrics(name, docs, result) {
    const seconds = result.durationMs / 1000;
    return {
        scenario: name,
        docs,
        emittedBytes: result.bytes,
        durationMs: Number(result.durationMs.toFixed(3)),
        docsPerSecond: Number((docs / seconds).toFixed(2)),
        mbPerSecond: Number(((result.bytes / (1024 * 1024)) / seconds).toFixed(2)),
        checksum: result.checksum
    };
}

async function main() {
    const streamCount = toNumber(process.env.BENCH_STREAMS, 12);
    const eventsPerStream = toNumber(process.env.BENCH_EVENTS_PER_STREAM, 10000);
    const payloadSize = toNumber(process.env.BENCH_PAYLOAD_SIZE, 512);
    const passes = toNumber(process.env.BENCH_PASSES, 8);
    const storageDirectory = await fs.mkdtemp(path.join(os.tmpdir(), 'event-storage-http-streaming-bench-'));

    try {
        const readStore = await createFixture(storageDirectory, streamCount, eventsPerStream, payloadSize);
        const { partitions, partitionBuffers } = await createPartitionData(readStore);
        const allEntries = collectEntries(readStore.storage.index);
        const firstPartitionId = allEntries[0].partition;
        const singlePartitionEntries = allEntries.filter(entry => entry.partition === firstPartitionId);
        const firstPartitionBuffer = partitionBuffers[firstPartitionId];
        const firstPartitionHeader = partitions[firstPartitionId].headerSize;

        const docsRawPartition = singlePartitionEntries.length * passes;
        const docsSingleNdjson = singlePartitionEntries.length * passes;
        const docsMixedNdjson = allEntries.length * passes;

        const rawResult = benchmarkRawFullPartition(firstPartitionBuffer, passes);
        const singleNdjsonResult = benchmarkNdjsonSectionsSingleFile(
            firstPartitionBuffer,
            firstPartitionHeader,
            singlePartitionEntries,
            passes
        );
        const mixedNdjsonResult = benchmarkNdjsonSectionsMixedFiles(
            allEntries,
            partitionBuffers,
            partitions,
            passes
        );

        const output = {
            parameters: {
                streamCount,
                eventsPerStream,
                totalEvents: streamCount * eventsPerStream,
                payloadSize,
                passes
            },
            results: [
                toMetrics('raw-full-file-format (single partition)', docsRawPartition, rawResult),
                toMetrics('ndjson-buffer-sections (single partition)', docsSingleNdjson, singleNdjsonResult),
                toMetrics('ndjson-buffer-sections (mixed partitions interleaved)', docsMixedNdjson, mixedNdjsonResult)
            ]
        };

        console.log(JSON.stringify(output, null, 2));
        readStore.close();
    } finally {
        await fs.rm(storageDirectory, { recursive: true, force: true });
    }
}

main().catch(error => {
    console.error(error);
    process.exitCode = 1;
});
