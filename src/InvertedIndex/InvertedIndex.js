import fs from 'fs';
import path from 'path';
import { writeSegment, SegmentReader } from './Segment.js';
import { SegmentPostings, BufferedPostings, TermCursor, OrCursor, AndCursor, EXHAUSTED } from './cursors.js';
import { assert } from '../utils/util.js';
import { ensureDirectory, resolvePath } from '../utils/fsUtil.js';

const MANIFEST_VERSION = 1;
const DEFAULT_FLUSH_THRESHOLD = 1000000;
const DEFAULT_MERGE_FACTOR = 10;

/**
 * Growable list of ascending positions for one term in the memtable.
 */
class PostingBuffer {

    constructor() {
        this.data = new Float64Array(4);
        this.length = 0;
    }

    push(position) {
        if (this.length === this.data.length) {
            const grown = new Float64Array(this.data.length * 2);
            grown.set(this.data);
            this.data = grown;
        }
        this.data[this.length++] = position;
    }

    get last() {
        return this.length > 0 ? this.data[this.length - 1] : 0;
    }

    appendFrom(from, target) {
        let low = 0, high = this.length;
        while (low < high) {
            const mid = (low + high) >>> 1;
            if (this.data[mid] < from) low = mid + 1; else high = mid;
        }
        for (let i = low; i < this.length; i++) target.push(this.data[i]);
        return target;
    }
}

function compareTermBuffers(a, b) {
    return Buffer.compare(a.term, b.term);
}

/**
 * An inverted index that maps terms (e.g. `type:OrderPlaced`, `tag:course/1`) to the ascending list of global
 * positions of the documents carrying them. All terms share a few immutable segment files plus an in-memory
 * memtable, instead of one file per term.
 *
 * Positions only ever grow, so posting lists only grow at their end. That makes flushing a sequential write
 * and merging adjacent segments a per-term concatenation. The index is derived data: the event log acts as
 * its write-ahead log, so after `open()` the caller re-adds everything after `persistedUntil`.
 */
class InvertedIndex {

    /**
     * @param {string} [name] The file name prefix for the manifest and segment files.
     * @param {object} [options]
     * @param {string} [options.dataDirectory] Directory for the index files. Default '.'.
     * @param {number} [options.flushThreshold] Number of buffered postings that triggers a flush. Default 1 000 000.
     * @param {number} [options.mergeFactor] Merge once more than this many segments exist. Default 10.
     * @param {boolean} [options.syncOnFlush] fsync segments and manifest on flush. Default true.
     */
    constructor(name = 'inverted', options = {}) {
        assert(typeof name === 'string' && name !== '' && !name.includes('/'), 'Invalid inverted index name.');
        this.name = name;
        this.dataDirectory = resolvePath(options.dataDirectory || '.');
        this.flushThreshold = options.flushThreshold ?? DEFAULT_FLUSH_THRESHOLD;
        this.mergeFactor = Math.max(2, options.mergeFactor ?? DEFAULT_MERGE_FACTOR);
        this.syncOnFlush = options.syncOnFlush ?? true;
        this.manifestFile = path.join(this.dataDirectory, name + '.manifest');
        this.opened = false;
    }

    isOpen() {
        return this.opened;
    }

    /**
     * Load the manifest and segment footers and remove segment files a crash left behind.
     *
     * @returns {number} The last position that is persisted. Everything after it must be re-added.
     */
    open() {
        if (this.opened) {
            return this.persistedUntil;
        }
        ensureDirectory(this.dataDirectory);
        const manifest = this.readManifest();
        this.persistedUntil = manifest.persistedUntil;
        this.nextSegmentNumber = manifest.nextSegmentNumber;
        this.segments = manifest.segments.map(file => new SegmentReader(path.join(this.dataDirectory, file)));
        this.removeOrphanedSegments(new Set(manifest.segments));
        this.memtable = new Map();
        this.memtablePostings = 0;
        this.lastAdded = this.persistedUntil;
        this.opened = true;
        return this.persistedUntil;
    }

    readManifest() {
        if (!fs.existsSync(this.manifestFile)) {
            return { persistedUntil: 0, nextSegmentNumber: 1, segments: [] };
        }
        const manifest = JSON.parse(fs.readFileSync(this.manifestFile, 'utf8'));
        assert(manifest.version === MANIFEST_VERSION, `Unsupported inverted index manifest version ${manifest.version}.`);
        return manifest;
    }

    writeManifest() {
        const manifest = {
            version: MANIFEST_VERSION,
            persistedUntil: this.persistedUntil,
            nextSegmentNumber: this.nextSegmentNumber,
            segments: this.segments.map(segment => path.basename(segment.fileName))
        };
        const tmpFile = this.manifestFile + '.tmp';
        const fd = fs.openSync(tmpFile, 'w');
        try {
            fs.writeSync(fd, JSON.stringify(manifest));
            if (this.syncOnFlush) fs.fsyncSync(fd);
        } finally {
            fs.closeSync(fd);
        }
        fs.renameSync(tmpFile, this.manifestFile);
        if (this.syncOnFlush) this.syncDirectory();
    }

    syncDirectory() {
        let fd;
        try {
            fd = fs.openSync(this.dataDirectory, 'r');
            fs.fsyncSync(fd);
        } catch (e) {
            // Directories can not be opened or fsynced on every platform (e.g. Windows).
        } finally {
            if (fd !== undefined) fs.closeSync(fd);
        }
    }

    segmentFilePattern() {
        return new RegExp('^' + this.name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\.seg-\\d+\\.inv$');
    }

    removeOrphanedSegments(liveFiles) {
        const pattern = this.segmentFilePattern();
        for (const file of fs.readdirSync(this.dataDirectory)) {
            if (pattern.test(file) && !liveFiles.has(file)) {
                fs.unlinkSync(path.join(this.dataDirectory, file));
            }
        }
        const tmpFile = this.manifestFile + '.tmp';
        if (fs.existsSync(tmpFile)) fs.unlinkSync(tmpFile);
    }

    nextSegmentFile() {
        const number = String(this.nextSegmentNumber++).padStart(6, '0');
        return path.join(this.dataDirectory, `${this.name}.seg-${number}.inv`);
    }

    /**
     * @returns {number} The last position added, persisted or not.
     */
    get length() {
        return this.lastAdded;
    }

    /**
     * Index a document position under the given terms.
     *
     * @param {number} position The global position. Must be greater than every position added before.
     * @param {Iterable<string>} terms
     */
    add(position, terms) {
        assert(this.opened, 'Inverted index is not open.');
        assert(Number.isSafeInteger(position) && position > this.lastAdded,
            `Positions must be added in strictly ascending order (got ${position} after ${this.lastAdded}).`);
        for (const term of terms) {
            let postings = this.memtable.get(term);
            if (postings === undefined) {
                postings = new PostingBuffer();
                this.memtable.set(term, postings);
            }
            if (postings.last !== position) {
                postings.push(position);
                this.memtablePostings++;
            }
        }
        this.lastAdded = position;
        if (this.memtablePostings >= this.flushThreshold) {
            this.flush();
        }
    }

    *sortedMemtable() {
        const terms = [];
        for (const [term, postings] of this.memtable) {
            terms.push({ term: Buffer.from(term, 'utf8'), positions: postings.data, length: postings.length });
        }
        yield* terms.sort(compareTermBuffers);
    }

    /**
     * Write the memtable as a new segment and record it in the manifest.
     *
     * @returns {boolean} True if anything was persisted.
     */
    flush() {
        if (!this.opened || this.lastAdded === this.persistedUntil) {
            return false;
        }
        if (this.memtable.size > 0) {
            const fileName = this.nextSegmentFile();
            writeSegment(fileName, { from: this.persistedUntil + 1, to: this.lastAdded }, this.sortedMemtable(), this.syncOnFlush);
            this.segments.push(new SegmentReader(fileName));
        }
        this.persistedUntil = this.lastAdded;
        this.writeManifest();
        this.memtable = new Map();
        this.memtablePostings = 0;
        this.maybeMerge();
        return true;
    }

    /**
     * Merge the `mergeFactor` adjacent segments with the smallest total size once there are too many.
     * Only adjacent segments are merged so that segment ranges stay disjoint and ascending.
     */
    maybeMerge() {
        while (this.segments.length > this.mergeFactor) {
            let bestStart = 0, bestSize = Infinity;
            for (let start = 0; start + this.mergeFactor <= this.segments.length; start++) {
                let size = 0;
                for (let i = start; i < start + this.mergeFactor; i++) size += this.segments[i].size;
                if (size < bestSize) {
                    bestSize = size;
                    bestStart = start;
                }
            }
            this.mergeSegments(bestStart, bestStart + this.mergeFactor);
        }
    }

    /**
     * @param {number} start First segment slot (inclusive).
     * @param {number} end Last segment slot (exclusive).
     */
    mergeSegments(start, end) {
        const inputs = this.segments.slice(start, end);
        const fileName = this.nextSegmentFile();
        writeSegment(fileName, { from: inputs[0].from, to: inputs[inputs.length - 1].to }, mergedTerms(inputs), this.syncOnFlush);
        this.segments.splice(start, end - start, new SegmentReader(fileName));
        this.writeManifest();
        for (const segment of inputs) {
            segment.close();
            fs.unlinkSync(segment.fileName);
        }
    }

    /**
     * @param {string} term
     * @param {number} [from=1] Only return positions >= from.
     * @returns {number[]} Ascending positions.
     */
    get(term, from = 1) {
        assert(this.opened, 'Inverted index is not open.');
        const result = [];
        const termBuffer = Buffer.from(term, 'utf8');
        for (const segment of this.segments) {
            if (segment.to < from) continue;
            const entry = segment.lookup(termBuffer);
            if (entry !== null && entry.lastPosition >= from) {
                segment.readPostings(entry, from, result);
            }
        }
        this.memtable.get(term)?.appendFrom(from, result);
        return result;
    }

    /**
     * @param {string} term
     * @returns {number} Number of positions indexed under the term.
     */
    count(term) {
        assert(this.opened, 'Inverted index is not open.');
        const termBuffer = Buffer.from(term, 'utf8');
        let count = this.memtable.get(term)?.length ?? 0;
        for (const segment of this.segments) {
            count += segment.lookup(termBuffer)?.docFreq ?? 0;
        }
        return count;
    }

    /**
     * @param {string} term
     * @returns {number} The highest position indexed under the term, or 0.
     */
    lastPosition(term) {
        assert(this.opened, 'Inverted index is not open.');
        const buffered = this.memtable.get(term);
        if (buffered !== undefined && buffered.length > 0) {
            return buffered.last;
        }
        const termBuffer = Buffer.from(term, 'utf8');
        for (let i = this.segments.length - 1; i >= 0; i--) {
            const entry = this.segments[i].lookup(termBuffer);
            if (entry !== null) return entry.lastPosition;
        }
        return 0;
    }

    /**
     * @param {string} term
     * @param {number} from
     * @returns {TermCursor}
     */
    termCursor(term, from) {
        const parts = [];
        const termBuffer = Buffer.from(term, 'utf8');
        for (const segment of this.segments) {
            if (segment.to < from) continue;
            const entry = segment.lookup(termBuffer);
            if (entry !== null && entry.lastPosition >= from) parts.push(new SegmentPostings(segment, entry));
        }
        const buffered = this.memtable.get(term);
        if (buffered !== undefined) parts.push(new BufferedPostings(buffered.data, buffered.length));
        return new TermCursor(parts);
    }

    /**
     * Build a cursor tree for a selector with the same algebra as `EventStore.fromStreams()`: arrays alternate
     * between OR (even depth, starting at the top level) and AND (odd depth); strings are terms.
     * AND nodes intersect by leapfrogging, so only the blocks of long posting lists that can contain a match
     * get decoded.
     *
     * @param {Array|string} node
     * @param {number} from
     * @param {number} [depth=0]
     * @returns {{ advance: function(number): number }}
     */
    cursor(node, from, depth = 0) {
        assert(this.opened, 'Inverted index is not open.');
        if (typeof node === 'string') {
            return this.termCursor(node, from);
        }
        assert(Array.isArray(node), 'Selector nodes must be strings or arrays.');
        const children = node.map(child => this.cursor(child, from, depth + 1));
        if (children.length === 1) {
            return children[0];
        }
        return depth % 2 === 0 ? new OrCursor(children) : new AndCursor(children);
    }

    /**
     * @param {Array|string} selector See cursor().
     * @param {number} [from=1]
     * @returns {number[]} Ascending positions.
     */
    query(selector, from = 1) {
        const root = this.cursor(selector, from);
        const result = [];
        for (let position = root.advance(from); position !== EXHAUSTED; position = root.advance(position + 1)) {
            result.push(position);
        }
        return result;
    }

    /**
     * DCB conflict check: did any document matching the selector get added after the given position?
     * Segments that end at or before `after` are never touched, so a recent condition is answered from
     * memory, and the search stops at the first match.
     *
     * @param {Array|string} selector See cursor().
     * @param {number} after
     * @returns {boolean}
     */
    hasMatchAfter(selector, after) {
        if (after >= this.lastAdded) {
            return false;
        }
        return this.cursor(selector, after + 1).advance(after + 1) !== EXHAUSTED;
    }

    /**
     * @param {string} [prefix='']
     * @returns {string[]} All known terms starting with the prefix, sorted by their UTF-8 bytes.
     */
    terms(prefix = '') {
        assert(this.opened, 'Inverted index is not open.');
        const prefixBuffer = Buffer.from(prefix, 'utf8');
        const found = new Map();
        for (const segment of this.segments) {
            for (const term of segment.termsWithPrefix(prefixBuffer)) found.set(term.toString('utf8'), term);
        }
        for (const term of this.memtable.keys()) {
            if (term.startsWith(prefix) && !found.has(term)) found.set(term, Buffer.from(term, 'utf8'));
        }
        return [...found.entries()].sort((a, b) => Buffer.compare(a[1], b[1])).map(([term]) => term);
    }

    /**
     * @returns {boolean} True if every segment's checksum matches.
     */
    verify() {
        return this.segments.every(segment => segment.verify());
    }

    /**
     * @returns {{ segments: number, bytes: number, memtableTerms: number, memtablePostings: number, persistedUntil: number }}
     */
    stats() {
        return {
            segments: this.segments.length,
            bytes: this.segments.reduce((sum, segment) => sum + segment.size, 0),
            memtableTerms: this.memtable.size,
            memtablePostings: this.memtablePostings,
            persistedUntil: this.persistedUntil
        };
    }

    /**
     * Flush the memtable and release all file handles.
     */
    close() {
        if (!this.opened) {
            return;
        }
        this.flush();
        for (const segment of this.segments) segment.close();
        this.segments = [];
        this.memtable = new Map();
        this.opened = false;
    }
}

/**
 * Zipper over the sorted dictionaries of adjacent segments. Because each segment's range lies completely
 * after the previous one, the merged posting list of a term is the concatenation in segment order.
 *
 * @param {SegmentReader[]} inputs
 */
function *mergedTerms(inputs) {
    const dictionaries = inputs.map(segment => segment.entries());
    const cursors = new Array(inputs.length).fill(0);
    for (;;) {
        let smallest = null;
        for (let i = 0; i < inputs.length; i++) {
            const entry = dictionaries[i][cursors[i]];
            if (entry !== undefined && (smallest === null || Buffer.compare(entry.term, smallest) < 0)) {
                smallest = entry.term;
            }
        }
        if (smallest === null) {
            return;
        }
        const positions = [];
        for (let i = 0; i < inputs.length; i++) {
            const entry = dictionaries[i][cursors[i]];
            if (entry !== undefined && Buffer.compare(entry.term, smallest) === 0) {
                inputs[i].readPostings(entry, 0, positions);
                cursors[i]++;
            }
        }
        yield { term: smallest, positions, length: positions.length };
    }
}

export default InvertedIndex;
