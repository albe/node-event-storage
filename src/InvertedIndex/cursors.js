import { ByteReader } from './encoding.js';
import { POSTINGS_BLOCK_SIZE } from './Segment.js';

const EXHAUSTED = Infinity;

/**
 * Seekable view on one term's postings inside one segment. Decodes only the blocks it lands on,
 * located through the skip table.
 */
class SegmentPostings {

    constructor(segment, entry) {
        this.segment = segment;
        this.entry = entry;
        this.reader = null;
    }

    load() {
        const buffer = this.segment.readRegion(this.entry.offset, this.entry.offset + this.entry.size);
        this.reader = new ByteReader(buffer);
        this.count = this.reader.varint();
        const blockCount = this.reader.varint();
        this.firsts = new Array(blockCount);
        this.starts = new Array(blockCount);
        let first = 0;
        let start = 0;
        const lengths = new Array(blockCount);
        for (let block = 0; block < blockCount; block++) {
            first += this.reader.varint();
            this.firsts[block] = first;
            lengths[block] = this.reader.varint();
        }
        const dataStart = this.reader.offset;
        for (let block = 0; block < blockCount; block++) {
            this.starts[block] = dataStart + start;
            start += lengths[block];
        }
        this.block = -1;
        this.decoded = [];
        this.cursor = 0;
    }

    decodeBlock(block) {
        this.block = block;
        this.reader.offset = this.starts[block];
        const length = Math.min(POSTINGS_BLOCK_SIZE, this.count - block * POSTINGS_BLOCK_SIZE);
        const decoded = new Array(length);
        let position = this.firsts[block];
        decoded[0] = position;
        for (let i = 1; i < length; i++) {
            position += this.reader.varint();
            decoded[i] = position;
        }
        this.decoded = decoded;
        this.cursor = 0;
    }

    /**
     * @param {number} target Must not be smaller than the target of the previous call.
     * @returns {number} The first position >= target, or Infinity.
     */
    advance(target) {
        if (target > this.entry.lastPosition) {
            return EXHAUSTED;
        }
        if (this.reader === null) {
            this.load();
        }
        let low = Math.max(this.block, 0), high = this.firsts.length - 1;
        while (low < high) {
            const mid = (low + high + 1) >>> 1;
            if (this.firsts[mid] <= target) low = mid; else high = mid - 1;
        }
        if (low !== this.block) {
            this.decodeBlock(low);
        }
        const decoded = this.decoded;
        while (this.cursor < decoded.length && decoded[this.cursor] < target) this.cursor++;
        if (this.cursor < decoded.length) {
            return decoded[this.cursor];
        }
        this.decodeBlock(low + 1);
        return this.decoded[0];
    }
}

class BufferedPostings {

    constructor(data, length) {
        this.data = data;
        this.length = length;
        this.cursor = 0;
    }

    advance(target) {
        let low = this.cursor, high = this.length;
        while (low < high) {
            const mid = (low + high) >>> 1;
            if (this.data[mid] < target) low = mid + 1; else high = mid;
        }
        this.cursor = low;
        return low < this.length ? this.data[low] : EXHAUSTED;
    }
}

/**
 * All postings of one term: segment parts in segment order, then the memtable part.
 */
class TermCursor {

    constructor(parts) {
        this.parts = parts;
        this.part = 0;
        this.current = -1;
    }

    advance(target) {
        if (this.current >= target) {
            return this.current;
        }
        while (this.part < this.parts.length) {
            const position = this.parts[this.part].advance(target);
            if (position !== EXHAUSTED) {
                return this.current = position;
            }
            this.part++;
        }
        return this.current = EXHAUSTED;
    }
}

class OrCursor {

    constructor(children) {
        this.children = children;
    }

    advance(target) {
        let smallest = EXHAUSTED;
        for (const child of this.children) {
            const position = child.advance(target);
            if (position < smallest) smallest = position;
        }
        return smallest;
    }
}

/**
 * Leapfrog intersection: every child seeks to the largest candidate until all agree.
 */
class AndCursor {

    constructor(children) {
        this.children = children;
    }

    advance(target) {
        let candidate = target;
        let agreeing = 0;
        let i = 0;
        while (agreeing < this.children.length) {
            const position = this.children[i].advance(candidate);
            if (position === EXHAUSTED) {
                return EXHAUSTED;
            }
            if (position === candidate) {
                agreeing++;
            } else {
                candidate = position;
                agreeing = 1;
            }
            i = (i + 1) % this.children.length;
        }
        return candidate;
    }
}

export { SegmentPostings, BufferedPostings, TermCursor, OrCursor, AndCursor, EXHAUSTED };
