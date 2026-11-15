/**
 * Measures warm EventStore reads over streams and joined streams (fromStreams).
 *
 * Usage: node --expose-gc bench-join.js [path/to/package-root] [events]
 * Run it once against the root of each version to compare. Reads run five times after one warm-up run.
 */
import fs from 'fs';
import path from 'path';
import { fileURLToPath, pathToFileURL } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(process.argv[2] || path.join(__dirname, '..'));
const eventCount = Number(process.argv[3] || 200000);
const storageDirectory = path.join(__dirname, 'data', 'join');
const { EventStore } = await import(pathToFileURL(path.join(root, 'index.js')).href);

function streamsOf(i) {
    // a: every 2nd event, b: every 3rd, rare: every 1000th, other: the rest
    const streams = [];
    if (i % 2 === 0) streams.push('a');
    if (i % 3 === 0) streams.push('b');
    if (i % 1000 === 0) streams.push('rare');
    return streams.length ? streams : ['other'];
}

async function openStore() {
    const store = new EventStore('bench', { storageDirectory });
    await new Promise(resolve => store.once('ready', resolve));
    return store;
}

fs.rmSync(storageDirectory, { recursive: true, force: true });
let store = await openStore();
// Every event goes to one physical stream; the joinable streams are matcher-based on payload.tags.
for (const name of ['a', 'b', 'rare']) {
    store.createEventStream(name, { payload: { tags: { $has: name } } });
}
for (let i = 1; i <= eventCount; i++) {
    store.commit('events', { tags: streamsOf(i), i });
}
store.close();
store = await openStore();

function count(stream) {
    let n = 0;
    for (const _ of stream) n++; // jshint ignore:line
    return n;
}

function first(stream) {
    for (const event of stream) return event ? 1 : 0;
    return 0;
}

function time(label, read) {
    const expected = read();
    const start = process.hrtime.bigint();
    for (let k = 0; k < 5; k++) {
        if (read() !== expected) throw new Error(`${label}: result changed`);
    }
    const ms = Number(process.hrtime.bigint() - start) / 5e6;
    console.log(`  ${label.padEnd(34)} ${ms.toFixed(2).padStart(9)} ms  (${expected} events)`);
}

console.log(`${root} (${eventCount} events, node ${process.version})`);
time('stream a', () => count(store.getEventStream('a')));
time('all events', () => count(store.getAllEvents()));
time('a OR b', () => count(store.fromStreams('j1', ['a', 'b'])));
time('a AND b', () => count(store.fromStreams('j2', [['a', 'b']])));
time('a AND rare', () => count(store.fromStreams('j3', [['a', 'rare']])));
time('a OR b, backwards', () => count(store.fromStreams('j4', ['a', 'b'], -1, 1)));
time('a OR b, first event only', () => first(store.fromStreams('j5', ['a', 'b'])));
global.gc();
const usage = process.memoryUsage();
console.log(`  ${'memory (heap + ArrayBuffers)'.padEnd(34)} ${((usage.heapUsed + usage.arrayBuffers) / 1048576).toFixed(1).padStart(9)} MB`);
store.close();
fs.rmSync(storageDirectory, { recursive: true, force: true });
