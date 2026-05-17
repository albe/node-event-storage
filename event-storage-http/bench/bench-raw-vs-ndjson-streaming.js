import fs from 'fs/promises';
import http from 'http';
import os from 'os';
import path from 'path';
import { once } from 'events';
import { performance } from 'perf_hooks';
import EventStore from '../../index.js';
import { RawEventStream } from '../../index.js';

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
    return { readStore, streams };
}

async function writeChunk(response, chunk) {
    if (response.write(chunk)) {
        return;
    }
    await once(response, 'drain');
}

async function streamResponse(response, source, endResponse = true) {
    for await (const chunk of source) {
        await writeChunk(response, chunk);
    }
    if (endResponse) {
        response.end();
    }
}

function createBenchmarkServer(readStore, firstStreamIndex) {
    return http.createServer((request, response) => {
        const requestUrl = new URL(request.url, 'http://127.0.0.1');
        const passes = toNumber(requestUrl.searchParams.get('passes'), 1);

        const handleRequest = async () => {
            switch (requestUrl.pathname) {
            case '/ndjson-single':
                response.writeHead(200, { 'content-type': 'application/x-ndjson; charset=utf-8' });
                for (let pass = 0; pass < passes; pass++) {
                    await streamResponse(response, new RawEventStream(readStore.storage, 1, -1, firstStreamIndex), pass === passes - 1);
                }
                break;
            case '/ndjson-mixed':
                response.writeHead(200, { 'content-type': 'application/x-ndjson; charset=utf-8' });
                for (let pass = 0; pass < passes; pass++) {
                    await streamResponse(response, new RawEventStream(readStore.storage, 1, -1), pass === passes - 1);
                }
                break;
            default:
                response.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
                response.end('Not found');
            }
        };

        void handleRequest().catch((error) => {
            response.statusCode = 500;
            response.end(String(error?.stack || error));
        });
    });
}

async function measureDownload(url) {
    const startedAt = performance.now();
    const response = await fetch(url);
    if (!response.ok) {
        throw new Error(`Unexpected status ${response.status} for ${url}`);
    }

    let bytes = 0;
    let checksum = 0;
    const reader = response.body.getReader();
    while (true) {
        const { done, value } = await reader.read();
        if (done) {
            break;
        }
        bytes += value.byteLength;
        checksum ^= value[0] || 0;
        checksum ^= value[value.byteLength - 1] || 0;
        checksum ^= value[value.byteLength >> 1] || 0;
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
        const { readStore, streams } = await createFixture(storageDirectory, streamCount, eventsPerStream, payloadSize);
        const allDocs = readStore.storage.index.length;
        const firstStreamIndex = readStore.getEventStream(streams[0]).streamIndex;
        const singleStreamDocs = firstStreamIndex.length;

        const server = createBenchmarkServer(readStore, firstStreamIndex);
        await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
        const address = server.address();
        const baseUrl = `http://127.0.0.1:${address.port}`;

        const singleNdjsonResult = await measureDownload(`${baseUrl}/ndjson-single?passes=${passes}`);
        const mixedNdjsonResult = await measureDownload(`${baseUrl}/ndjson-mixed?passes=${passes}`);

        await new Promise(resolve => server.close(resolve));
        readStore.close();

        console.log(JSON.stringify({
            parameters: {
                streamCount,
                eventsPerStream,
                totalEvents: streamCount * eventsPerStream,
                payloadSize,
                passes
            },
            results: [
                toMetrics('ndjson-buffer-sections (single stream)', singleStreamDocs * passes, singleNdjsonResult),
                toMetrics('ndjson-buffer-sections (all streams interleaved)', allDocs * passes, mixedNdjsonResult)
            ]
        }, null, 2));
    } finally {
        await fs.rm(storageDirectory, { recursive: true, force: true });
    }
}

main().catch(error => {
    console.error(error);
    process.exitCode = 1;
});

