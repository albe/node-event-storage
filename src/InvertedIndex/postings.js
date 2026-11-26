import { ByteReader } from './encoding.js';
import { POSTINGS_BLOCK_SIZE } from './Segment.js';

/*
 * Lazy ranges over the postings of one term, in the protocol of `index.range()`: they yield a reused
 * `{ number }` object in ascending order, and `next(number)` skips ahead to the first posting >= number.
 * That makes them combinable with `union` and `intersect` from `utils/indexUtil.js`.
 */

/**
 * Read the skip table of one term's postings in a segment: the first position and byte offset of every block.
 *
 * @param {SegmentReader} segment
 * @param {object} entry The term's dictionary entry.
 * @returns {{ reader: ByteReader, count: number, firsts: number[], starts: number[] }}
 */
function readSkipTable(segment, entry) {
    const reader = new ByteReader(segment.readRegion(entry.offset, entry.offset + entry.size));
    const count = reader.varint();
    const blockCount = reader.varint();
    const firsts = new Array(blockCount);
    const lengths = new Array(blockCount);
    let first = 0;
    for (let block = 0; block < blockCount; block++) {
        first += reader.varint();
        firsts[block] = first;
        lengths[block] = reader.varint();
    }
    const starts = new Array(blockCount);
    let start = reader.offset;
    for (let block = 0; block < blockCount; block++) {
        starts[block] = start;
        start += lengths[block];
    }
    return { reader, count, firsts, starts };
}

/**
 * @param {{ reader: ByteReader, count: number, firsts: number[], starts: number[] }} table
 * @param {number} block
 * @returns {number[]} The positions of one block.
 */
function decodeBlock(table, block) {
    const { reader, firsts } = table;
    reader.offset = table.starts[block];
    const length = Math.min(POSTINGS_BLOCK_SIZE, table.count - block * POSTINGS_BLOCK_SIZE);
    const positions = new Array(length);
    let position = positions[0] = firsts[block];
    for (let i = 1; i < length; i++) {
        position += reader.varint();
        positions[i] = position;
    }
    return positions;
}

/**
 * @param {number[]} firsts
 * @param {number} from The block to start searching at.
 * @param {number} target
 * @returns {number} The last block whose first position is <= target, but at least `from`.
 */
function blockFor(firsts, from, target) {
    let low = from, high = firsts.length - 1;
    while (low < high) {
        const mid = (low + high + 1) >>> 1;
        if (firsts[mid] <= target) low = mid; else high = mid - 1;
    }
    return low;
}

/**
 * @param {ArrayLike<number>} positions Ascending positions.
 * @param {number} low
 * @param {number} high
 * @param {number} target
 * @returns {number} The first index in [low, high) whose position is >= target, or high.
 */
function firstAtLeast(positions, low, high, target) {
    while (low < high) {
        const mid = (low + high) >>> 1;
        if (positions[mid] < target) low = mid + 1; else high = mid;
    }
    return low;
}

/**
 * Postings of one term inside one segment. Only the blocks it lands on get decoded, located through the skip table.
 *
 * @param {SegmentReader} segment
 * @param {object} entry The term's dictionary entry.
 * @param {number} from The lowest position to yield.
 * @returns {Generator<{ number: number }>}
 */
function* segmentPostings(segment, entry, from) {
    if (entry.lastPosition < from) {
        return;
    }
    const table = readSkipTable(segment, entry);
    const hit = { number: 0 };
    let target = from;
    let block = blockFor(table.firsts, 0, target);
    while (block < table.firsts.length) {
        const positions = decodeBlock(table, block);
        let i = firstAtLeast(positions, 0, positions.length, target);
        while (i < positions.length) {
            hit.number = positions[i];
            target = Math.max((yield hit) ?? 0, positions[i] + 1);
            if (block + 1 < table.firsts.length && table.firsts[block + 1] <= target) {
                break;
            }
            i = firstAtLeast(positions, i + 1, positions.length, target);
        }
        if (target > entry.lastPosition) {
            return;
        }
        block = i < positions.length ? blockFor(table.firsts, block + 1, target) : block + 1;
    }
}

/**
 * Postings of one term in the memtable.
 *
 * @param {number[]} positions Ascending positions.
 * @param {number} from The lowest position to yield.
 * @returns {Generator<{ number: number }>}
 */
function* bufferedPostings(positions, from) {
    const length = positions.length;
    const hit = { number: 0 };
    let i = firstAtLeast(positions, 0, length, from);
    while (i < length) {
        hit.number = positions[i];
        const target = yield hit;
        i = target === undefined ? i + 1 : firstAtLeast(positions, i + 1, length, target);
    }
}

/**
 * Chain the posting ranges of one term from consecutive segments and the memtable. A part is only opened once the
 * previous one is exhausted, starting at the highest skip target seen so far.
 *
 * @param {Array<function(number): Generator>} parts Opens the range of one part from a position on.
 * @param {number} from
 * @returns {Generator<{ number: number }>}
 */
function* chainPostings(parts, from) {
    for (const open of parts) {
        const range = open(from);
        let result = range.next();
        while (!result.done) {
            const target = yield result.value;
            from = Math.max(result.value.number + 1, target ?? 0);
            result = range.next(target);
        }
    }
}

export { segmentPostings, bufferedPostings, chainPostings };
