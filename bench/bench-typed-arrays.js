/**
 * Index-entry storage: Array of Entry objects (current) vs. a plain number Array vs. a Uint32Array.
 * Each entry has 4 uint32 fields (number, position, size, partition) like IndexEntry.
 *
 * Usage: node --expose-gc bench-typed-arrays.js [entries=1000000]
 * Every variant runs in its own child process, so shared call sites never turn polymorphic across variants.
 */
import { execFileSync } from 'child_process';
import { fileURLToPath } from 'url';
import Entry from '../src/IndexEntry.js';

const N = Number(process.argv[2] || 1000000);
const LOOKUPS = 1000000;
const REPEATS = 7;
const FIELDS = 4;

function median(values) {
    const sorted = [...values].sort((a, b) => a - b);
    return sorted[sorted.length >> 1];
}

function measure(fn) {
    fn();
    const times = [];
    let sink = 0;
    for (let i = 0; i < REPEATS; i++) {
        const start = process.hrtime.bigint();
        sink += fn();
        times.push(Number(process.hrtime.bigint() - start) / 1e6);
    }
    if (sink === -1) console.log(sink);
    return median(times);
}

function randomIndices() {
    const indices = new Uint32Array(LOOKUPS);
    let seed = 1;
    for (let i = 0; i < LOOKUPS; i++) {
        seed = (seed * 1103515245 + 12345) >>> 0;
        indices[i] = seed % N;
    }
    return indices;
}

function fileBuffer() {
    const buffer = Buffer.alloc(N * Entry.size);
    for (let i = 0; i < N; i++) new Entry(i + 1, i * 100, 100, 7).toBuffer(buffer, i * Entry.size);
    return buffer;
}

const variants = {
    'Array<Entry>': {
        build() {
            const data = [];
            for (let i = 0; i < N; i++) data.push(new Entry(i + 1, i * 100, 100, 7));
            return data;
        },
        load(source) {
            const data = new Array(N);
            for (let i = 0; i < N; i++) data[i] = Entry.fromBuffer(source, i * Entry.size);
            return data;
        },
        number: (data, i) => data[i].number,
        position: (data, i) => data[i].position
    },
    'Array<Uint32Array(4)>': {
        build() {
            const data = [];
            for (let i = 0; i < N; i++) data.push(Uint32Array.of(i + 1, i * 100, 100, 7));
            return data;
        },
        load(source) {
            const data = new Array(N);
            for (let i = 0; i < N; i++) data[i] = new Uint32Array(source.buffer, source.byteOffset + i * Entry.size, FIELDS);
            return data;
        },
        number: (data, i) => data[i][0],
        position: (data, i) => data[i][1]
    },
    'Array<number>': {
        build() {
            const data = [];
            for (let i = 0; i < N; i++) data.push(i + 1, i * 100, 100, 7);
            return data;
        },
        load(source) {
            const data = new Array(N * FIELDS);
            for (let i = 0; i < N * FIELDS; i++) data[i] = source.readUInt32LE(i * 4);
            return data;
        },
        number: (data, i) => data[i * FIELDS],
        position: (data, i) => data[i * FIELDS + 1]
    },
    'Uint32Array': {
        build() {
            let data = new Uint32Array(1024);
            let length = 0;
            for (let i = 0; i < N; i++) {
                if (length + FIELDS > data.length) {
                    const grown = new Uint32Array(data.length * 2);
                    grown.set(data);
                    data = grown;
                }
                data[length++] = i + 1;
                data[length++] = i * 100;
                data[length++] = 100;
                data[length++] = 7;
            }
            return data.subarray(0, length);
        },
        load(source) {
            // Zero-copy view; little-endian hosts only, which covers every platform Node ships for today.
            return new Uint32Array(source.buffer, source.byteOffset, N * FIELDS);
        },
        number: (data, i) => data[i * FIELDS],
        position: (data, i) => data[i * FIELDS + 1]
    }
};

function heapPerEntry(build) {
    global.gc?.();
    const before = process.memoryUsage();
    globalThis.keep = build();
    global.gc?.();
    const after = process.memoryUsage();
    const bytes = (after.heapUsed - before.heapUsed) + (after.arrayBuffers - before.arrayBuffers);
    globalThis.keep = null;
    return bytes / N;
}

function binarySearch(data, number, getNumber) {
    let low = 0, high = N - 1;
    while (low <= high) {
        const mid = (low + high) >>> 1;
        const value = getNumber(data, mid);
        if (value === number) return mid;
        if (value < number) low = mid + 1; else high = mid - 1;
    }
    return -1;
}

function runVariant(name) {
    const indices = randomIndices();
    const buffer = fileBuffer();
    const variant = variants[name];
    const data = variant.build();
    const { number, position } = variant;
    return {
        'build (ms)': measure(() => variant.build().length),
        'load from file buffer (ms)': measure(() => variant.load(buffer).length),
        'sequential read (ms)': measure(() => {
            let sum = 0;
            for (let i = 0; i < N; i++) sum += number(data, i) + position(data, i);
            return sum;
        }),
        'random read (ms)': measure(() => {
            let sum = 0;
            for (let i = 0; i < LOOKUPS; i++) sum += position(data, indices[i]);
            return sum;
        }),
        'binary search (ms)': measure(() => {
            let sum = 0;
            for (let i = 0; i < LOOKUPS; i++) sum += binarySearch(data, indices[i] + 1, number);
            return sum;
        }),
        'memory (bytes/entry)': heapPerEntry(() => variant.build())
    };
}

if (process.argv[3]) {
    process.stdout.write(JSON.stringify(runVariant(process.argv[3])));
} else {
    const names = Object.keys(variants);
    const results = {};
    for (const name of names) {
        const output = execFileSync(process.execPath, ['--expose-gc', fileURLToPath(import.meta.url), String(N), name]);
        results[name] = JSON.parse(output.toString());
    }
    console.log(`node ${process.version}, ${N} entries, ${LOOKUPS} lookups, median of ${REPEATS}`);
    console.log('| Operation | ' + names.join(' | ') + ' |');
    console.log('| --- |' + names.map(() => ' ---: |').join(''));
    for (const metric of Object.keys(results[names[0]])) {
        console.log(`| ${metric} | ` + names.map(name => results[name][metric].toFixed(1)).join(' | ') + ' |');
    }
}
