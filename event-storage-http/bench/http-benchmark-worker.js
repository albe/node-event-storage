import { performance } from 'node:perf_hooks';
import HttpEventStream from '../src/HttpEventStream.js';

const configuration = JSON.parse(process.argv[2] ?? '{}');

await configureHttpDispatcher(configuration.concurrency ?? 1);

function createWorkers(count, createWork) {
    return Array.from({ length: count }, (_, index) => createWork(index));
}

async function configureHttpDispatcher(concurrency) {
    try {
        const { Agent, setGlobalDispatcher } = await import('undici');
        setGlobalDispatcher(new Agent({
            connections: Math.max(16, concurrency * 4),
            pipelining: 1,
            keepAliveTimeout: 60_000,
            keepAliveMaxTimeout: 60_000
        }));
    } catch {
        return undefined;
    }
}

function ensureSuccessfulResponse(response, requestDescription) {
    if (response.ok) {
        return;
    }
    throw new Error(`${requestDescription} failed with ${response.status} ${response.statusText}`);
}

function buildWriteRequestBody(requestIndex, laneIndex, eventPayload) {
    return JSON.stringify({
        events: [{
            type: 'BenchEvent',
            seq: requestIndex,
            lane: laneIndex,
            payload: eventPayload
        }]
    });
}

function resolveWriteStreamName(laneIndex, config) {
    if (config.writeTarget === 'single-stream') {
        return 'write-bench';
    }
    return `write-bench-${laneIndex + 1}`;
}

async function runWriteRequest(requestIndex, laneIndex, config) {
    const streamName = resolveWriteStreamName(laneIndex, config);
    const response = await fetch(`${config.baseUrl}/streams/${streamName}/commit`, {
        method: 'POST',
        headers: {
            'content-type': 'application/json'
        },
        body: buildWriteRequestBody(requestIndex, laneIndex, config.eventPayload)
    });
    ensureSuccessfulResponse(response, `POST /streams/${streamName}/commit`);
    const commit = await response.json();
    if (!Array.isArray(commit.events) || commit.events.length !== 1) {
        throw new Error('Commit response did not contain exactly one event.');
    }
    return 1;
}

async function runReadRequest(config) {
    const response = await fetch(`${config.baseUrl}${config.path}`);
    ensureSuccessfulResponse(response, config.path);
    const stream = new HttpEventStream(response);
    let eventCount = 0;
    for await (const event of stream) {
        if (!event || typeof event !== 'object') {
            throw new Error('Read response yielded a non-object event.');
        }
        eventCount++;
    }
    if (eventCount !== config.expectedEvents) {
        throw new Error(`Expected ${config.expectedEvents} events but received ${eventCount}.`);
    }
    return eventCount;
}

async function warmUp(config) {
    if (config.mode === 'write') {
        await runWriteRequest(-1, 0, config);
        return;
    }
    await runReadRequest(config);
}

async function runTimedRequests(config) {
    let nextRequestIndex = 0;
    const laneTotals = await Promise.all(createWorkers(config.concurrency, async laneIndex => {
        let laneTotal = 0;
        while (true) {
            const requestIndex = nextRequestIndex++;
            if (requestIndex >= config.totalRequests) {
                return laneTotal;
            }
            laneTotal += config.mode === 'write'
                ? await runWriteRequest(requestIndex, laneIndex, config)
                : await runReadRequest(config);
        }
    }));
    return laneTotals.reduce((sum, value) => sum + value, 0);
}

async function main() {
    if (!configuration.baseUrl) {
        throw new Error('baseUrl is required.');
    }
    if (!Number.isInteger(configuration.concurrency) || configuration.concurrency <= 0) {
        throw new Error('concurrency must be a positive integer.');
    }
    if (!Number.isInteger(configuration.totalRequests) || configuration.totalRequests <= 0) {
        throw new Error('totalRequests must be a positive integer.');
    }

    await warmUp(configuration);
    const startedAt = performance.now();
    const totalEvents = await runTimedRequests(configuration);
    const elapsedMs = performance.now() - startedAt;

    process.stdout.write(JSON.stringify({
        concurrency: configuration.concurrency,
        totalRequests: configuration.totalRequests,
        totalEvents,
        elapsedMs,
        rate: totalEvents / (elapsedMs / 1000)
    }));
}

main().catch(error => {
    console.error(error);
    process.exit(1);
});


