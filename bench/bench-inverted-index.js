/**
 * Compares the InvertedIndex with the existing one-file-per-term indexes (as EventStore creates them for
 * `typeAccessor` / `tagsAccessor` streams) on a DCB course-subscription workload.
 *
 * Usage: node --expose-gc bench-inverted-index.js [events=1000000] [courses=1000] [students=10000]
 * Each variant runs in its own child process so heap numbers do not influence each other.
 */
import fs from 'fs';
import path from 'path';
import { execFileSync } from 'child_process';
import { fileURLToPath } from 'url';
import Index from '../src/Index.js';
import FileHandlePool from '../src/FileHandlePool.js';
import InvertedIndex from '../src/InvertedIndex.js';
import { resolveSelector } from '../src/utils/indexUtil.js';

const __filename = fileURLToPath(import.meta.url);
const dataRoot = path.join(path.dirname(__filename), 'data', 'inverted-bench');

const COURSE_TYPES = ['type:CourseDefined', 'type:CourseCapacityChanged', 'type:StudentSubscribedToCourse', 'type:StudentUnsubscribed'];
const STUDENT_TYPES = ['type:StudentRegistered', 'type:StudentSubscribedToCourse', 'type:StudentUnsubscribed'];
const QUERIES = 2000;
const MAX_OPEN_INDEXES = 1024;

/**
 * mulberry32: a small seeded PRNG whose consecutive values are not correlated modulo small numbers.
 */
function createRandom(seed) {
    return (n) => {
        seed = (seed + 0x6D2B79F5) | 0;
        let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
        t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
        return (((t ^ (t >>> 14)) >>> 0) / 4294967296 * n) | 0;
    };
}

function* generateEvents(count, courses, students) {
    const random = createRandom(7);
    for (let position = 1; position <= count; position++) {
        const r = random(100);
        const course = 'tag:course/' + random(courses);
        const student = 'tag:student/' + random(students);
        if (r < 5) yield ['type:CourseDefined', course];
        else if (r < 10) yield ['type:CourseCapacityChanged', course];
        else if (r < 25) yield ['type:StudentRegistered', student];
        else if (r < 90) yield ['type:StudentSubscribedToCourse', course, student];
        else yield ['type:StudentUnsubscribed', course, student];
    }
}

function decisionSelectors(courses, students) {
    const random = createRandom(99);
    const selectors = [];
    for (let i = 0; i < QUERIES; i++) {
        selectors.push([
            ['tag:course/' + random(courses), COURSE_TYPES],
            ['tag:student/' + random(students), STUDENT_TYPES]
        ]);
    }
    return selectors;
}

function directorySize(directory) {
    let files = 0, bytes = 0;
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
        const full = path.join(directory, entry.name);
        if (entry.isDirectory()) {
            const sub = directorySize(full);
            files += sub.files;
            bytes += sub.bytes;
        } else {
            files++;
            bytes += fs.statSync(full).size;
        }
    }
    return { files, bytes };
}

function heapMB() {
    global.gc?.();
    return process.memoryUsage().heapUsed / 1024 / 1024;
}

function time(fn) {
    const start = process.hrtime.bigint();
    const result = fn();
    return { ms: Number(process.hrtime.bigint() - start) / 1e6, result };
}

/**
 * The existing approach: one WritableIndex file per term, sharing an LRU file-handle pool like Storage does.
 */
class FilePerTermIndex {

    constructor(directory) {
        this.directory = directory;
        this.pool = new FileHandlePool(MAX_OPEN_INDEXES);
        this.indexes = new Map();
    }

    fileFor(term) {
        return term.replace(/[:/]/g, '_') + '.index';
    }

    get(term, create) {
        let index = this.indexes.get(term);
        if (index === undefined) {
            const fileName = this.fileFor(term);
            if (!create && !fs.existsSync(path.join(this.directory, fileName))) return null;
            index = new Index(fileName, { dataDirectory: this.directory, fileHandlePool: this.pool });
            index.open();
            this.indexes.set(term, index);
        }
        return index;
    }

    add(position, terms) {
        const entry = new Index.Entry(position, position * 100, 100, 0);
        for (const term of terms) this.get(term, true).add(entry);
    }

    range(term, from) {
        const index = this.get(term, false);
        const start = index === null ? 0 : index.find(from, true);
        if (start < 1 || start > index.length) return [][Symbol.iterator]();
        return index.range(start);
    }

    query(selector, from = 1) {
        return Array.from(resolveSelector(selector, term => this.range(term, from)), entry => entry.number);
    }

    hasMatchAfter(selector, after) {
        return !resolveSelector(selector, term => this.range(term, after + 1)).next().done;
    }

    startup() {
        // Storage only scans the directory and opens secondary indexes lazily on first access.
        return fs.readdirSync(this.directory).length;
    }

    close() {
        // Flush explicitly: WritableIndex.close() drops buffered entries when the pool already evicted its handle.
        for (const index of this.indexes.values()) {
            index.flush();
            index.close();
        }
        this.indexes.clear();
    }
}

class InvertedVariant {

    constructor(directory) {
        this.index = new InvertedIndex('bench', { dataDirectory: directory });
        this.index.open();
    }

    add(position, terms) { this.index.add(position, terms); }
    query(selector, from = 1) { return Array.from(this.index.query(selector, from), hit => hit.number); }
    hasMatchAfter(selector, after) { return this.index.hasMatchAfter(selector, after); }
    startup() { return this.index.stats().segments; }
    close() { this.index.close(); }
}

const VARIANTS = {
    'file-per-term': FilePerTermIndex,
    'inverted': InvertedVariant
};

function averageMs(selectors, fn) {
    const { ms } = time(() => selectors.forEach(fn));
    return ms / selectors.length;
}

function runVariant(name, count, courses, students) {
    const directory = path.join(dataRoot, name);
    fs.rmSync(directory, { recursive: true, force: true });
    fs.mkdirSync(directory, { recursive: true });
    const Variant = VARIANTS[name];

    let postings = 0;
    const write = time(() => {
        const variant = new Variant(directory);
        let position = 0;
        for (const terms of generateEvents(count, courses, students)) {
            variant.add(++position, terms);
            postings += terms.length;
        }
        variant.close();
    });
    const disk = directorySize(directory);

    const heapBefore = heapMB();
    let variant;
    const startup = time(() => {
        variant = new Variant(directory);
        variant.startup();
    });
    const selectors = decisionSelectors(courses, students);
    const firstQuery = time(() => variant.query(selectors[0]));
    const coldQuery = averageMs(selectors.slice(1), selector => variant.query(selector));
    const warmQuery = averageMs(selectors, selector => variant.query(selector));
    const conflictCheck = averageMs(selectors, selector => variant.hasMatchAfter(selector, count - 10));
    const checksum = selectors.slice(0, 50).reduce((sum, selector) => sum + variant.query(selector).length, 0);
    const heap = heapMB() - heapBefore;
    variant.close();

    return {
        writeMs: write.ms,
        writePerEventUs: write.ms * 1000 / count,
        postings,
        files: disk.files,
        diskMB: disk.bytes / 1024 / 1024,
        bytesPerPosting: disk.bytes / postings,
        startupMs: startup.ms,
        firstQueryMs: firstQuery.ms,
        coldQueryMs: coldQuery,
        warmQueryMs: warmQuery,
        conflictCheckMs: conflictCheck,
        heapMB: heap,
        checksum
    };
}

function format(value, digits = 2) {
    return typeof value === 'number' ? value.toLocaleString('en-US', { maximumFractionDigits: digits }) : String(value);
}

function main() {
    const [count = 1000000, courses = 1000, students = 10000] = process.argv.slice(2).map(Number);
    if (process.argv[2] === 'run') {
        const [, , , name, n, c, s] = process.argv;
        process.stdout.write(JSON.stringify(runVariant(name, Number(n), Number(c), Number(s))));
        return;
    }
    console.log(`Events: ${count}, courses: ${courses}, students: ${students}, queries: ${QUERIES}, node ${process.version}`);
    const results = {};
    for (const name of Object.keys(VARIANTS)) {
        const output = execFileSync(process.execPath, ['--expose-gc', __filename, 'run', name, count, courses, students], { maxBuffer: 1 << 20 });
        results[name] = JSON.parse(output.toString());
    }
    if (results['file-per-term'].checksum !== results.inverted.checksum) {
        throw new Error('Variants returned different query results.');
    }
    const rows = [
        ['Write all events (ms)', 'writeMs'],
        ['Write per event (µs)', 'writePerEventUs'],
        ['Postings', 'postings'],
        ['Files on disk', 'files'],
        ['Size on disk (MB)', 'diskMB'],
        ['Bytes per posting', 'bytesPerPosting'],
        ['Startup (ms)', 'startupMs'],
        ['First DCB query (ms)', 'firstQueryMs'],
        ['Cold DCB query, avg (ms)', 'coldQueryMs'],
        ['Warm DCB query, avg (ms)', 'warmQueryMs'],
        ['Conflict check, avg (ms)', 'conflictCheckMs'],
        ['Heap after queries (MB)', 'heapMB']
    ];
    console.log('| Metric | file-per-term | inverted |');
    console.log('| --- | ---: | ---: |');
    for (const [label, key] of rows) {
        console.log(`| ${label} | ${format(results['file-per-term'][key])} | ${format(results.inverted[key])} |`);
    }
    fs.rmSync(dataRoot, { recursive: true, force: true });
}

main();
