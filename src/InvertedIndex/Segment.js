import fs from 'fs';
import { ByteWriter, ByteReader, crc32 } from './encoding.js';

const SEGMENT_MAGIC = 'nesinv01';
const SEGMENT_VERSION = 1;
const FOOTER_SIZE = 64;
const POSTINGS_BLOCK_SIZE = 128;
const DICTIONARY_BLOCK_SIZE = 64;
const DICTIONARY_CACHE_SIZE = 256;

class CorruptSegmentError extends Error {}

/**
 * Postings layout: count, block count, skip table (first position delta, block byte length) per block,
 * then the blocks holding the remaining positions of each block as deltas.
 * The skip table lets a reader start decoding at the block that contains a requested start position.
 *
 * @param {ByteWriter} writer
 * @param {ArrayLike<number>} positions Strictly ascending.
 * @param {number} length Number of valid entries in `positions`.
 * @param {ByteWriter} scratch Reused buffer for encoding the blocks before their lengths are known.
 */
function encodePostings(writer, positions, length, scratch) {
    const blockCount = Math.ceil(length / POSTINGS_BLOCK_SIZE);
    writer.varint(length);
    writer.varint(blockCount);
    scratch.length = 0;
    const blockEnds = [];
    for (let start = 0; start < length; start += POSTINGS_BLOCK_SIZE) {
        const end = Math.min(start + POSTINGS_BLOCK_SIZE, length);
        for (let i = start + 1; i < end; i++) {
            scratch.varint(positions[i] - positions[i - 1]);
        }
        blockEnds.push(scratch.length);
    }
    let previousFirst = 0;
    let previousEnd = 0;
    for (let block = 0; block < blockCount; block++) {
        const first = positions[block * POSTINGS_BLOCK_SIZE];
        writer.varint(first - previousFirst);
        writer.varint(blockEnds[block] - previousEnd);
        previousFirst = first;
        previousEnd = blockEnds[block];
    }
    writer.bytes(scratch.buffer, 0, scratch.length);
}

function readSkipTable(reader) {
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
    return { count, firsts, lengths };
}

/**
 * @param {Buffer} buffer The encoded postings of one term.
 * @param {number} from Only positions >= from are returned.
 * @param {number[]} target Positions are appended to this array.
 */
function decodePostings(buffer, from, target) {
    const reader = new ByteReader(buffer);
    const { count, firsts, lengths } = readSkipTable(reader);
    let startBlock = 0;
    while (startBlock + 1 < firsts.length && firsts[startBlock + 1] <= from) startBlock++;
    for (let block = 0; block < startBlock; block++) reader.offset += lengths[block];

    for (let block = startBlock; block < firsts.length; block++) {
        let position = firsts[block];
        if (position >= from) target.push(position);
        const blockLength = Math.min(POSTINGS_BLOCK_SIZE, count - block * POSTINGS_BLOCK_SIZE);
        for (let i = 1; i < blockLength; i++) {
            position += reader.varint();
            if (position >= from) target.push(position);
        }
    }
    return target;
}

function commonPrefixLength(a, b) {
    const max = Math.min(a.length, b.length);
    let i = 0;
    while (i < max && a[i] === b[i]) i++;
    return i;
}

/**
 * Write a complete segment file. Terms must be sorted by `Buffer.compare` of their UTF-8 bytes.
 *
 * @param {string} fileName
 * @param {{ from: number, to: number }} range The position range (inclusive) this segment covers.
 * @param {Iterable<{ term: Buffer, positions: ArrayLike<number>, length: number }>} sortedTerms
 * @param {boolean} [sync=true] Whether to fsync the file before returning.
 * @returns {number} The file size in bytes.
 */
function writeSegment(fileName, range, sortedTerms, sync = true) {
    const writer = new ByteWriter(64 * 1024);
    const scratch = new ByteWriter(4096);
    const dictionary = [];
    let postingCount = 0;

    writer.bytes(Buffer.from(SEGMENT_MAGIC));
    for (const { term, positions, length } of sortedTerms) {
        const offset = writer.length;
        encodePostings(writer, positions, length, scratch);
        dictionary.push({ term, docFreq: length, lastPosition: positions[length - 1], offset, size: writer.length - offset });
        postingCount += length;
    }

    const dictOffset = writer.length;
    const sparse = [];
    let previous = Buffer.alloc(0);
    for (let i = 0; i < dictionary.length; i++) {
        const entry = dictionary[i];
        const blockStart = i % DICTIONARY_BLOCK_SIZE === 0;
        if (blockStart) {
            sparse.push({ term: entry.term, offset: writer.length });
            previous = Buffer.alloc(0);
        }
        const shared = commonPrefixLength(previous, entry.term);
        writer.varint(shared);
        writer.varint(entry.term.length - shared);
        writer.bytes(entry.term, shared);
        writer.varint(entry.docFreq);
        writer.varint(entry.lastPosition);
        writer.varint(entry.offset);
        writer.varint(entry.size);
        previous = entry.term;
    }

    const sparseOffset = writer.length;
    for (const { term, offset } of sparse) {
        writer.varint(term.length);
        writer.bytes(term);
        writer.varint(offset);
    }

    const checksum = crc32(writer.toBuffer());
    writer.doubleLE(range.from);
    writer.doubleLE(range.to);
    writer.doubleLE(dictOffset);
    writer.doubleLE(sparseOffset);
    writer.uint32LE(dictionary.length);
    writer.uint32LE(sparse.length);
    writer.doubleLE(postingCount);
    writer.uint32LE(checksum);
    writer.uint32LE(SEGMENT_VERSION);
    writer.bytes(Buffer.from(SEGMENT_MAGIC));

    const fd = fs.openSync(fileName, 'w');
    try {
        fs.writeSync(fd, writer.buffer, 0, writer.length, 0);
        if (sync) fs.fsyncSync(fd);
    } finally {
        fs.closeSync(fd);
    }
    return writer.length;
}

/**
 * Read access to one immutable segment file. Only the footer and the sparse dictionary index stay in memory.
 */
class SegmentReader {

    constructor(fileName) {
        this.fileName = fileName;
        this.fd = fs.openSync(fileName, 'r');
        this.size = fs.fstatSync(this.fd).size;
        this.dictionaryCache = new Map();
        try {
            this.readFooter();
            this.readSparseIndex();
        } catch (e) {
            this.close();
            throw e;
        }
    }

    readRegion(start, end) {
        const buffer = Buffer.allocUnsafe(end - start);
        const read = fs.readSync(this.fd, buffer, 0, buffer.length, start);
        if (read !== buffer.length) {
            throw new CorruptSegmentError(`Segment ${this.fileName} is truncated.`);
        }
        return buffer;
    }

    readFooter() {
        if (this.size < FOOTER_SIZE + SEGMENT_MAGIC.length) {
            throw new CorruptSegmentError(`Segment ${this.fileName} is too small.`);
        }
        this.footerOffset = this.size - FOOTER_SIZE;
        const footer = this.readRegion(this.footerOffset, this.size);
        if (footer.toString('latin1', 56, 64) !== SEGMENT_MAGIC || footer.readUInt32LE(52) !== SEGMENT_VERSION) {
            throw new CorruptSegmentError(`Segment ${this.fileName} has an invalid footer.`);
        }
        this.from = footer.readDoubleLE(0);
        this.to = footer.readDoubleLE(8);
        this.dictOffset = footer.readDoubleLE(16);
        this.sparseOffset = footer.readDoubleLE(24);
        this.termCount = footer.readUInt32LE(32);
        this.dictionaryBlockCount = footer.readUInt32LE(36);
        this.postingCount = footer.readDoubleLE(40);
        this.checksum = footer.readUInt32LE(48);
    }

    readSparseIndex() {
        const reader = new ByteReader(this.readRegion(this.sparseOffset, this.footerOffset));
        this.blockTerms = new Array(this.dictionaryBlockCount);
        this.blockOffsets = new Array(this.dictionaryBlockCount + 1);
        for (let block = 0; block < this.dictionaryBlockCount; block++) {
            this.blockTerms[block] = Buffer.from(reader.bytes(reader.varint()));
            this.blockOffsets[block] = reader.varint();
        }
        this.blockOffsets[this.dictionaryBlockCount] = this.sparseOffset;
    }

    /**
     * Verify the checksum over the whole segment. Reads the complete file.
     *
     * @returns {boolean}
     */
    verify() {
        return crc32(this.readRegion(0, this.footerOffset)) === this.checksum;
    }

    decodeDictionaryRegion(buffer, target) {
        const reader = new ByteReader(buffer);
        let previous = Buffer.alloc(0);
        let index = 0;
        while (reader.offset < buffer.length) {
            if (index % DICTIONARY_BLOCK_SIZE === 0) previous = Buffer.alloc(0);
            const shared = reader.varint();
            const suffix = reader.bytes(reader.varint());
            const term = Buffer.concat([previous.subarray(0, shared), suffix]);
            target.push({
                term,
                docFreq: reader.varint(),
                lastPosition: reader.varint(),
                offset: reader.varint(),
                size: reader.varint()
            });
            previous = term;
            index++;
        }
        return target;
    }

    dictionaryBlock(block) {
        let entries = this.dictionaryCache.get(block);
        if (entries) {
            return entries;
        }
        entries = this.decodeDictionaryRegion(this.readRegion(this.blockOffsets[block], this.blockOffsets[block + 1]), []);
        if (this.dictionaryCache.size >= DICTIONARY_CACHE_SIZE) {
            this.dictionaryCache.delete(this.dictionaryCache.keys().next().value);
        }
        this.dictionaryCache.set(block, entries);
        return entries;
    }

    /**
     * @param {Buffer} term
     * @returns {number} The last dictionary block whose first term is <= term, or -1.
     */
    findBlock(term) {
        let low = 0, high = this.dictionaryBlockCount - 1, found = -1;
        while (low <= high) {
            const mid = (low + high) >>> 1;
            if (Buffer.compare(this.blockTerms[mid], term) <= 0) {
                found = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return found;
    }

    /**
     * @param {Buffer} term
     * @returns {{ term: Buffer, docFreq: number, lastPosition: number, offset: number, size: number }|null}
     */
    lookup(term) {
        const block = this.findBlock(term);
        if (block < 0) {
            return null;
        }
        const entries = this.dictionaryBlock(block);
        let low = 0, high = entries.length - 1;
        while (low <= high) {
            const mid = (low + high) >>> 1;
            const comparison = Buffer.compare(entries[mid].term, term);
            if (comparison === 0) return entries[mid];
            if (comparison < 0) low = mid + 1; else high = mid - 1;
        }
        return null;
    }

    /**
     * @param {object} entry A dictionary entry returned by lookup().
     * @param {number} from
     * @param {number[]} target
     */
    readPostings(entry, from, target) {
        return decodePostings(this.readRegion(entry.offset, entry.offset + entry.size), from, target);
    }

    /**
     * All dictionary entries in term order.
     *
     * @returns {object[]}
     */
    entries() {
        return this.decodeDictionaryRegion(this.readRegion(this.dictOffset, this.sparseOffset), []);
    }

    /**
     * @param {Buffer} prefix
     * @returns {Buffer[]} All terms starting with the prefix, in term order.
     */
    termsWithPrefix(prefix) {
        const result = [];
        for (let block = Math.max(0, this.findBlock(prefix)); block < this.dictionaryBlockCount; block++) {
            for (const { term } of this.dictionaryBlock(block)) {
                if (term.length >= prefix.length && prefix.compare(term, 0, prefix.length) === 0) {
                    result.push(term);
                } else if (Buffer.compare(term, prefix) > 0) {
                    return result;
                }
            }
        }
        return result;
    }

    close() {
        if (this.fd !== null) {
            fs.closeSync(this.fd);
            this.fd = null;
        }
        this.dictionaryCache.clear();
    }
}

export { writeSegment, SegmentReader, CorruptSegmentError, encodePostings, decodePostings, POSTINGS_BLOCK_SIZE };
