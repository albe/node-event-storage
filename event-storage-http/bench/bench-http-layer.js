import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { once } from 'node:events';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import EventStoreHttpApi from '../src/EventStoreHttpApi.js';

const host = '127.0.0.1';
const concurrencyLevels = [1, 2, 4, 8, 16];
const benchmarkMode = readBenchmarkMode(process.argv[2] ?? process.env.HTTP_BENCH_MODE);
const readEventCount = readPositiveInteger(process.env.HTTP_BENCH_READ_EVENTS, 10_000);
const readRequestsPerLane = readPositiveInteger(process.env.HTTP_BENCH_READ_REQUESTS_PER_LANE, 4);
const writeRequestsPerLane = readPositiveInteger(process.env.HTTP_BENCH_WRITE_REQUESTS_PER_LANE, 400);
const eventPayload = 'x'.repeat(460);
const workerPath = path.join(path.dirname(fileURLToPath(import.meta.url)), 'http-benchmark-worker.js');

async function loadEventStore() {
    try {
        const module = await import('event-storage');
        return module.default ?? module.EventStore;
    } catch {
        const module = await import('../../index.js');
        return module.default ?? module.EventStore;
    }
}

function readPositiveInteger(rawValue, defaultValue) {
    if (rawValue === undefined) {
        return defaultValue;
    }
    const parsedValue = Number(rawValue);
    if (!Number.isInteger(parsedValue) || parsedValue <= 0) {
        throw new Error(`Expected a positive integer but got "${rawValue}".`);
    }
    return parsedValue;
}

function readBenchmarkMode(rawValue) {
    if (rawValue === undefined) {
        return 'all';
    }
    const normalizedValue = String(rawValue).trim().toLowerCase();
    if (normalizedValue === 'all' || normalizedValue === 'write' || normalizedValue === 'read') {
        return normalizedValue;
    }
    throw new Error(`Expected HTTP_BENCH_MODE to be one of "all", "write", or "read", but got "${rawValue}".`);
}

function commitAsync(eventStore, streamName, events) {
    return new Promise((resolve, reject) => {
        try {
            eventStore.commit(streamName, events, resolve);
        } catch (error) {
            reject(error);
        }
    });
}

async function createEventStore(storageDirectory) {
    const EventStore = await loadEventStore();
    const eventStore = new EventStore({
        storageDirectory,
        typeAccessor: 'type'
    });
    await once(eventStore, 'ready');
    return eventStore;
}

async function createFixture(fixtureName) {
    const storageDirectory = await fs.mkdtemp(path.join(os.tmpdir(), `event-storage-http-bench-${fixtureName}-`));
    const eventStore = await createEventStore(storageDirectory);
    const api = new EventStoreHttpApi(eventStore);
    const server = api.createServer();
    await new Promise(resolve => server.listen(0, host, resolve));
    const address = server.address();
    return {
        storageDirectory,
        eventStore,
        server,
        baseUrl: `http://${host}:${address.port}`
    };
}

async function destroyFixture(fixture) {
    await new Promise((resolve, reject) => {
        fixture.server.close(error => error ? reject(error) : resolve());
    });
    fixture.eventStore.close();
    await fs.rm(fixture.storageDirectory, { recursive: true, force: true });
}

async function populateReadFixture(eventStore) {
    for (let index = 0; index < readEventCount; index++) {
        await commitAsync(eventStore, index % 2 === 0 ? 'stream-1' : 'stream-2', [{
            type: 'BenchEvent',
            seq: index,
            payload: eventPayload
        }]);
    }
}

function getReadScenarios() {
    const third = Math.ceil(readEventCount / 3);
    const twoThirds = Math.floor(2 * readEventCount / 3);
    return [
        {
            name: '1 - forward full scan',
            path: '/streams/category/stream',
            expectedEvents: readEventCount
        },
        {
            name: '2 - backwards full scan',
            path: `/streams/category/stream/backwards/${readEventCount}`,
            expectedEvents: readEventCount
        },
        {
            name: '3 - join stream',
            path: '/streams/join?streams=stream-1,stream-2',
            expectedEvents: readEventCount
        },
        {
            name: '4 - range scan',
            path: `/streams/category/stream/from/${third}/until/${twoThirds}`,
            expectedEvents: twoThirds - third + 1
        }
    ];
}

function formatRate(rate) {
    return Math.round(rate).toLocaleString('en-US');
}

function renderTable(title, rowNames, rateMatrix) {
    const headers = ['scenario', ...concurrencyLevels.map(String)];
    const rows = rowNames.map((rowName, rowIndex) => [
        rowName,
        ...concurrencyLevels.map((concurrency, columnIndex) => formatRate(rateMatrix[rowIndex][columnIndex].rate))
    ]);
    const columnWidths = headers.map((header, columnIndex) => Math.max(
        header.length,
        ...rows.map(row => row[columnIndex].length)
    ));

    console.log(`\n${title}`);
    console.log(headers.map((header, index) => header.padEnd(columnWidths[index])).join(' | '));
    console.log(columnWidths.map(width => '-'.repeat(width)).join('-|-'));
    for (const row of rows) {
        console.log(row.map((value, index) => value.padEnd(columnWidths[index])).join(' | '));
    }
}

async function runWorker(config) {
    const rawConfig = JSON.stringify(config);
    return new Promise((resolve, reject) => {
        const child = spawn(process.execPath, [workerPath, rawConfig], {
            stdio: ['ignore', 'pipe', 'pipe']
        });
        let stdout = '';
        let stderr = '';

        child.stdout.on('data', chunk => {
            stdout += chunk.toString();
        });
        child.stderr.on('data', chunk => {
            stderr += chunk.toString();
        });
        child.on('error', reject);
        child.on('close', code => {
            if (code !== 0) {
                const error = new Error(stderr || stdout || `Benchmark worker exited with code ${code}.`);
                reject(error);
                return;
            }
            try {
                resolve(JSON.parse(stdout));
            } catch (error) {
                reject(new Error(`Failed to parse worker output: ${stdout || stderr}\n${error.message}`));
            }
        });
    });
}

async function runWriteBenchmarks() {
    const scenarios = [
        {
            name: 'single-event commit (sharded streams)',
            writeTarget: 'per-lane-stream'
        },
        {
            name: 'single-event commit (single stream)',
            writeTarget: 'single-stream'
        }
    ];
    const rowNames = scenarios.map(scenario => scenario.name);
    const rateMatrix = scenarios.map(() => []);

    for (let scenarioIndex = 0; scenarioIndex < scenarios.length; scenarioIndex++) {
        const scenario = scenarios[scenarioIndex];
        for (const concurrency of concurrencyLevels) {
            const fixture = await createFixture(`write-${scenario.writeTarget}-${concurrency}`);
            try {
                const result = await runWorker({
                    mode: 'write',
                    baseUrl: fixture.baseUrl,
                    concurrency,
                    totalRequests: concurrency * writeRequestsPerLane,
                    eventPayload,
                    writeTarget: scenario.writeTarget
                });
                rateMatrix[scenarioIndex].push(result);
            } finally {
                await destroyFixture(fixture);
            }
        }
    }

    return { rowNames, rateMatrix };
}

async function runReadBenchmarks() {
    const fixture = await createFixture('read');
    try {
        await populateReadFixture(fixture.eventStore);
        const scenarios = getReadScenarios();
        const rowNames = scenarios.map(scenario => scenario.name);
        const rateMatrix = scenarios.map(() => []);

        for (let scenarioIndex = 0; scenarioIndex < scenarios.length; scenarioIndex++) {
            const scenario = scenarios[scenarioIndex];
            for (const concurrency of concurrencyLevels) {
                const result = await runWorker({
                    mode: 'read',
                    baseUrl: fixture.baseUrl,
                    concurrency,
                    totalRequests: concurrency * readRequestsPerLane,
                    path: scenario.path,
                    expectedEvents: scenario.expectedEvents
                });
                rateMatrix[scenarioIndex].push(result);
            }
        }

        return { rowNames, rateMatrix };
    } finally {
        await destroyFixture(fixture);
    }
}

function printRuntimeConfiguration() {
    console.log('HTTP layer benchmark over local loopback');
    console.log('The client load runs in a separate process, so the numbers include HTTP round-trips over 127.0.0.1.');
    console.log(`Benchmark mode: ${benchmarkMode}`);
    console.log(`Read fixture events: ${readEventCount.toLocaleString('en-US')}`);
    console.log(`Read requests per concurrency lane: ${readRequestsPerLane}`);
    console.log(`Write requests per concurrency lane: ${writeRequestsPerLane}`);
}

async function main() {
    printRuntimeConfiguration();
    if (benchmarkMode === 'all' || benchmarkMode === 'write') {
        const writeResults = await runWriteBenchmarks();
        renderTable('Write performance (events/s, 1 event/commit)', writeResults.rowNames, writeResults.rateMatrix);
    }

    if (benchmarkMode === 'all' || benchmarkMode === 'read') {
        const readResults = await runReadBenchmarks();
        renderTable('Read performance (events/s)', readResults.rowNames, readResults.rateMatrix);
    }
}

main().catch(error => {
    console.error(error);
    process.exit(1);
});



