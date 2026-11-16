/**
 * Measures write speed, heap usage and read speed of the index on a large index file.
 *
 * Usage: node --expose-gc bench-index-memory.js [path/to/src] [entries]
 * Run it once against the src directory of each version to compare. Every scenario runs in a fresh process.
 */
import fs from 'fs';
import path from 'path';
import { execFileSync } from 'child_process';
import { fileURLToPath, pathToFileURL } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const srcDir = path.resolve(process.argv[2] || path.join(__dirname, '..', 'src'));
const entryCount = Number(process.argv[3] || 1000000);
const scenario = process.argv[4];
const dataDirectory = path.join(__dirname, 'data', 'index-memory');

function mulberry32(seed) {
    return function () {
        seed |= 0; seed = seed + 0x6D2B79F5 | 0;
        let t = Math.imul(seed ^ seed >>> 15, 1 | seed);
        t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t;
        return ((t ^ t >>> 14) >>> 0) / 4294967296;
    };
}

function memoryMB() {
    global.gc();
    global.gc();
    const usage = process.memoryUsage();
    // Chunks live in ArrayBuffers, which heapUsed does not count.
    return (usage.heapUsed + usage.arrayBuffers) / 1024 / 1024;
}

function timed(fn) {
    const start = process.hrtime.bigint();
    const result = fn();
    return [Number(process.hrtime.bigint() - start) / 1e6, result];
}

/**
 * Read entries as an array: through the array-returning range() of older versions, otherwise with get().
 */
function readRange(index, from, until) {
    if (index.all) {
        return index.range(from, until);
    }
    const entries = new Array(until - from + 1);
    for (let position = from; position <= until; position++) {
        entries[position - from] = index.get(position);
    }
    return entries;
}

const scenarios = {
    write(Index) {
        fs.rmSync(dataDirectory, { recursive: true, force: true });
        const index = new Index('bench.index', { dataDirectory, writeBufferSize: 64 * 1024 });
        const [ms] = timed(() => {
            for (let i = 1; i <= entryCount; i++) {
                index.add(new Index.Entry(2 * i, i * 100, 100, i % 4));
            }
            index.flush();
        });
        index.close();
        return { 'write ms': ms };
    },
    memory(Index) {
        const before = memoryMB();
        const index = new Index('bench.index', { dataDirectory });
        const [ms] = timed(() => {
            for (let i = 1; i <= entryCount; i += 1000) {
                readRange(index, i, Math.min(entryCount, i + 999));
            }
        });
        const heap = memoryMB() - before;
        index.close();
        return { 'full read ms': ms, 'memory MB': heap, 'bytes/entry': heap * 1024 * 1024 / entryCount };
    },
    find(Index) {
        const index = new Index('bench.index', { dataDirectory });
        const random = mulberry32(42);
        const numbers = Array.from({ length: 1000000 }, () => 1 + Math.floor(random() * 2 * entryCount));
        const [coldMs] = timed(() => { for (let i = 0; i < 1000; i++) index.find(numbers[i]); });
        let checksum = 0;
        const [warmMs] = timed(() => { for (const number of numbers) checksum += index.find(number); });
        index.close();
        return { 'find 1k cold ms': coldMs, 'find 1M warm ms': warmMs, checksum };
    },
    get(Index) {
        const index = new Index('bench.index', { dataDirectory });
        readRange(index, 1, entryCount);
        const random = mulberry32(7);
        const positions = Array.from({ length: 1000000 }, () => 1 + Math.floor(random() * entryCount));
        let checksum = 0;
        const [ms] = timed(() => { for (const position of positions) checksum += index.get(position).position; });
        index.close();
        return { 'get 1M warm ms': ms, checksum };
    },
    range(Index) {
        const index = new Index('bench.index', { dataDirectory });
        readRange(index, 1, entryCount);
        let checksum = 0;
        const [smallMs] = timed(() => {
            for (let i = 1; i + 9 <= entryCount; i += 10) checksum += readRange(index, i, i + 9).length;
        });
        const [fullMs] = timed(() => { checksum += readRange(index, 1, entryCount).length; });
        index.close();
        return { 'range 10er warm ms': smallMs, 'range all warm ms': fullMs, checksum };
    },
    open(Index) {
        const [ms] = timed(() => {
            for (let i = 0; i < 1000; i++) {
                const index = new Index('bench.index', { dataDirectory });
                index.lastEntry;
                index.close();
            }
        });
        return { '1000x open+lastEntry ms': ms };
    }
};

if (scenario) {
    const { default: Index } = await import(pathToFileURL(path.join(srcDir, 'Index.js')).href);
    console.log(JSON.stringify(scenarios[scenario](Index)));
} else {
    const results = {};
    for (const name of Object.keys(scenarios)) {
        const output = execFileSync(process.execPath, ['--expose-gc', fileURLToPath(import.meta.url), srcDir, String(entryCount), name], { encoding: 'utf8' });
        Object.assign(results, JSON.parse(output.trim().split('\n').pop()));
    }
    console.log(`${srcDir} (${entryCount} entries, node ${process.version})`);
    for (const [key, value] of Object.entries(results)) {
        console.log(`  ${key.padEnd(24)} ${typeof value === 'number' && !Number.isInteger(value) ? value.toFixed(1) : value}`);
    }
}
