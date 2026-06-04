import { Readable, Transform, Writable } from 'stream';
import { pipeline } from 'stream/promises';
import { performance } from 'perf_hooks';
import { buildRawBufferMatcher, matches } from '../src/utils/metadataUtil.js';

const TARGET_MB = 32;
const TARGET_BYTES = TARGET_MB * 1024 * 1024;
const WARMUP_RUNS = 1;
const MEASURED_RUNS = 5;

const matcherObjects = [
    { name: 'flat',     matcher: { type: 'Foo' } },
    { name: 'scoped',   matcher: { payload: { type: 'Foo' } } },
    { name: 'multi',    matcher: { payload: { type: ['Foo', 'Baz'] } } },
    { name: '$gt', 		matcher: { payload: { value: { $gt: 100 } } } },
    { name: '$eq',      matcher: { payload: { type: { $eq: 'Foo' } } } }
];

const implementations = item => [
	{ name: `${item.name}-obj`, impl: 'obj', fn: (doc) => matches(doc, item.matcher) },
	{ name: `${item.name}-raw`, impl: 'raw', fn: buildRawBufferMatcher(item.matcher) },
	{ name: `${item.name}-+obj`, impl: '+obj', fn: (buffer) => matches(JSON.parse(buffer.toString('utf8')), item.matcher) }
];
const implementationsCount = implementations(matcherObjects[0]).length; // assuming all have same count

const matchers = matcherObjects.flatMap(implementations);

function createEventBuffer(i, matchFlag) {
    const doc = {
        type: matchFlag ? 'Foo' : 'Bar',
        id: i,
        payload: { type: matchFlag ? 'Foo' : 'Bar', value: matchFlag ? 1000 : 10 },
        body: 'x'.repeat(160),
        metadata: { commitVersion: i % 1000 }
    };
    return Buffer.from(JSON.stringify(doc), 'utf8');
}

function buildDataset(matchRatio) {
    const buffers = [];
    const objects = [];  // pre-parsed; each entry: { doc, len }
    let totalBytes = 0;
    let i = 0;
    while (totalBytes < TARGET_BYTES) {
        const matchFlag = (i % 100) < matchRatio;
        const buffer = createEventBuffer(i, matchFlag);
        buffers.push(buffer);
        objects.push({ doc: JSON.parse(buffer.toString('utf8')), len: buffer.length });
        totalBytes += buffer.length;
        i++;
    }
    return { buffers, objects, totalBytes };
}

async function runScenario(matchRatio, matcherName, matcherFn, impl, dataset) {
    const { buffers, objects, totalBytes } = dataset;
    const items = impl === 'obj' ? objects : buffers;
    let passedBytes = 0;
    let passedEvents = 0;
    const filter = new Transform({
        objectMode: true,
        transform(chunk, encoding, callback) {
            const matches = impl === 'obj' ? matcherFn(chunk.doc) : matcherFn(chunk);
            if (matches) {
                passedBytes += impl === 'obj' ? chunk.len : chunk.length;
                passedEvents++;
                this.push(chunk);
            }
            callback();
        }
    });
    const sink = new Writable({
        objectMode: true,
        write(chunk, encoding, callback) { callback(); }
    });
    const startedAt = performance.now();
    await pipeline(Readable.from(items), filter, sink);
    const elapsedSeconds = (performance.now() - startedAt) / 1000;
    const throughput = (totalBytes / (1024 * 1024)) / elapsedSeconds;
    return {
        matcherName,
        matchRatio,
        inputMB: totalBytes / (1024 * 1024),
        elapsedSeconds,
        throughput,
        passedBytes,
        passedEvents,
        eventsSeconds: passedEvents / elapsedSeconds
    };
}

function summarizeResults(matchRatio, matcherName, runs) {
    const throughputs = runs.map(run => run.throughput).sort((a, b) => a - b);
    const sum = throughputs.reduce((acc, value) => acc + value, 0);
    const avg = sum / throughputs.length;
    const variance = throughputs.reduce((acc, value) => acc + ((value - avg) ** 2), 0) / throughputs.length;

    return {
        matchRatio,
        matcherName,
        inputMB: runs[0].inputMB,
        passedBytes: runs[0].passedBytes,
        passedEvents: runs[0].passedEvents,
        eventsSeconds: runs.reduce((acc, value) => acc + value.eventsSeconds, 0) / runs.length,
        avgThroughput: avg,
        medianThroughput: throughputs[Math.floor(throughputs.length / 2)],
        minThroughput: throughputs[0],
        maxThroughput: throughputs[throughputs.length - 1],
        stdDevThroughput: Math.sqrt(variance),
        stdDevPercent: avg === 0 ? 0 : (Math.sqrt(variance) / avg) * 100,
        runs: throughputs
    };
}

function pad(value, width = 7) {
    return String(value).padStart(width, ' ');
}

function padr(value, width = 7) {
    return String(value).padEnd(width, ' ');
}

function formatNumber(value, width, unit = '', decimals = 1) {
    if (typeof unit === 'number') { decimals = unit; unit = ''; }
    return pad(value.toFixed(decimals) + unit, width);
}

function printTable(matchRatio, results) {
    // Δmed: within each matcher group, compare every later implementation against the first one.
    const deltas = new Map();
    for (let i = 0; i < results.length; i += implementationsCount) {
        const base = results[i];
        if (!base) {
            continue;
        }
        for (let j = 1; j < implementationsCount && i + j < results.length; j++) {
            const result = results[i + j];
            deltas.set(result.matcherName, ((result.medianThroughput - base.medianThroughput) / base.medianThroughput) * 100);
        }
    }

    const header = [
        padr('obj', 9),
        pad('impl', 5),
        pad('med', 7),
        pad('Δmed', 8),
        pad('avg', 7),
        pad('min', 7),
        pad('max', 7),
        pad('std%', 7),
        pad('passMB', 7),
        pad('events/s', 8)
    ].join(' ');
    const separator = '-'.repeat(header.length);

    console.log(`\n${matchRatio}% match`);
    console.log(header);
    console.log(separator);

    for (let i = 0; i < results.length; i++) {
        const result = results[i];
        const dashIdx = result.matcherName.lastIndexOf('-');
        const objectName = result.matcherName.slice(0, dashIdx);
        const implName = result.matcherName.slice(dashIdx + 1);
        const deltaValue = deltas.get(result.matcherName);
        const delta = deltaValue !== undefined
            ? `${deltaValue >= 0 ? '+' : ''}${deltaValue.toFixed(1)}%`
            : '';
        console.log([
            padr(objectName, 9),
            pad(implName, 5),
            formatNumber(result.medianThroughput, 7),
            pad(delta, 8),
            formatNumber(result.avgThroughput, 7),
            formatNumber(result.minThroughput, 7),
            formatNumber(result.maxThroughput, 7),
            formatNumber(result.stdDevPercent, 7, '%'),
            formatNumber(result.passedBytes / (1024 * 1024), 7),
            formatNumber(result.eventsSeconds / 1000, 8, 'k')
        ].join(' '));

        if ((i % implementationsCount) === implementationsCount - 1 && i < results.length - 1) {
            console.log('');
        }
    }
}

async function main() {
    console.log(`input=${TARGET_MB}MB warmup=${WARMUP_RUNS} runs=${MEASURED_RUNS}`);

    for (const ratio of [20, 50, 80]) {
        const dataset = buildDataset(ratio);
        const results = [];

        for (const matcher of matchers) {
            for (let i = 0; i < WARMUP_RUNS; i++) {
                await runScenario(ratio, matcher.name, matcher.fn, matcher.impl, dataset);
            }
            const runs = [];
            for (let i = 0; i < MEASURED_RUNS; i++) {
                runs.push(await runScenario(ratio, matcher.name, matcher.fn, matcher.impl, dataset));
            }
            results.push(summarizeResults(ratio, matcher.name, runs));
        }

        printTable(ratio, results);
    }
}

main().catch(err => {
    console.error(err);
    process.exitCode = 1;
});
