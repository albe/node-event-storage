import os from 'os';
import Entry from '../IndexEntry.js';

/**
 * Entries per chunk. A full chunk of default entries is 64 KiB, which keeps a lazy chunk load to one small read
 * and keeps the cost of growing the last chunk bounded.
 */
const CHUNK_CAPACITY = 4096;
const MIN_CHUNK_ALLOCATION = 8;
const LITTLE_ENDIAN_HOST = os.endianness() === 'LE';

/**
 * @param {number} count
 * @returns {number} The amount of entries to allocate room for, so a growing chunk doubles instead of copying on every append.
 */
function allocationFor(count) {
    let capacity = MIN_CHUNK_ALLOCATION;
    while (capacity < count) {
        capacity *= 2;
    }
    return Math.min(capacity, CHUNK_CAPACITY);
}

/**
 * A chunk holds the raw on-disk bytes of up to CHUNK_CAPACITY consecutive entries.
 * Only the first `filled` entries are valid; the rest is room to grow into.
 */
class Chunk {

    /**
     * @param {number} entrySize
     * @param {number} capacity Entries to allocate room for.
     */
    constructor(entrySize, capacity) {
        // A dedicated ArrayBuffer keeps the Uint32Array view aligned, which a pooled Buffer does not guarantee.
        const arrayBuffer = new ArrayBuffer(capacity * entrySize);
        this.buffer = Buffer.from(arrayBuffer);
        this.words = new Uint32Array(arrayBuffer);
        this.capacity = capacity;
        this.filled = 0;
    }

    /**
     * @param {number} entrySize
     * @param {number} capacity
     * @returns {Chunk} A larger chunk holding the same valid entries.
     */
    grow(entrySize, capacity) {
        const grown = new Chunk(entrySize, capacity);
        this.buffer.copy(grown.buffer, 0, 0, this.filled * entrySize);
        grown.filled = this.filled;
        return grown;
    }
}

/**
 * Stores index entries in fixed-size chunks of raw entry bytes instead of one object per entry.
 * This needs about 16 bytes per default entry instead of ~90, and appending never copies more than one chunk.
 * Chunks are read lazily from the index file with a single read each.
 */
class EntryChunks {

    /**
     * @param {typeof EntryInterface} EntryClass
     * @param {function(Buffer, number, number, number): number} readEntries Reads `length` bytes of entries starting at
     *        the given 0-based entry number into the buffer at the given offset and returns the amount of bytes read.
     */
    constructor(EntryClass, readEntries) {
        this.EntryClass = EntryClass;
        this.entrySize = EntryClass.size;
        this.readEntries = readEntries;
        // Entries with the default binary layout can be read straight from the 32-bit words without creating objects.
        this.wordsPerEntry = (LITTLE_ENDIAN_HOST && EntryClass.fromBuffer === Entry.fromBuffer) ? this.entrySize / 4 : 0;
        this.clear();
    }

    /**
     * Forget all entries.
     */
    clear() {
        /* @type Array<Chunk|undefined> */
        this.chunks = [];
        this.length = 0;
    }

    /**
     * Change the amount of entries. Growing only records the new length, entries are read on access.
     *
     * @param {number} length
     */
    setLength(length) {
        if (length < this.length) {
            this.truncate(length);
            return;
        }
        this.length = length;
    }

    /**
     * Drop all entries from the given 0-based entry number on.
     *
     * @param {number} length The amount of entries to keep.
     */
    truncate(length) {
        this.length = Math.min(this.length, length);
        const chunkCount = Math.ceil(this.length / CHUNK_CAPACITY);
        this.chunks.length = Math.min(this.chunks.length, chunkCount);
        const lastChunk = this.chunks[chunkCount - 1];
        if (lastChunk) {
            lastChunk.filled = Math.min(lastChunk.filled, this.length - (chunkCount - 1) * CHUNK_CAPACITY);
        }
    }

    /**
     * Return the chunk that holds the given entry, reading it from file if necessary.
     *
     * @private
     * @param {number} index The 0-based entry number.
     * @returns {Chunk|null} The chunk or null if the entry could not be read.
     */
    chunkFor(index) {
        const chunkIndex = Math.floor(index / CHUNK_CAPACITY);
        const chunk = this.chunks[chunkIndex];
        if (chunk && index - chunkIndex * CHUNK_CAPACITY < chunk.filled) {
            return chunk;
        }
        return this.load(chunkIndex, index);
    }

    /**
     * Read the missing tail of a chunk from file, up to the end of the chunk or the index.
     *
     * @private
     * @param {number} chunkIndex
     * @param {number} index The 0-based entry number that needs to be available afterwards.
     * @returns {Chunk|null}
     */
    load(chunkIndex, index) {
        const chunkStart = chunkIndex * CHUNK_CAPACITY;
        const count = Math.min(CHUNK_CAPACITY, this.length - chunkStart);
        let chunk = this.chunks[chunkIndex];
        if (!chunk) {
            chunk = new Chunk(this.entrySize, allocationFor(count));
        } else if (chunk.capacity < count) {
            chunk = chunk.grow(this.entrySize, allocationFor(count));
        }
        this.chunks[chunkIndex] = chunk;

        const offset = chunk.filled * this.entrySize;
        const bytesRead = this.readEntries(chunk.buffer, offset, (count - chunk.filled) * this.entrySize, chunkStart + chunk.filled);
        chunk.filled += Math.floor(bytesRead / this.entrySize);
        return index - chunkStart < chunk.filled ? chunk : null;
    }

    /**
     * @param {number} index The 0-based entry number.
     * @returns {EntryInterface|false} The entry or false if it could not be read.
     */
    get(index) {
        const chunk = this.chunkFor(index);
        if (!chunk) {
            return false;
        }
        const position = index % CHUNK_CAPACITY;
        if (this.wordsPerEntry) {
            const word = position * this.wordsPerEntry;
            const words = chunk.words;
            return new this.EntryClass(words[word], words[word + 1], words[word + 2], words[word + 3]);
        }
        return this.EntryClass.fromBuffer(chunk.buffer, position * this.entrySize);
    }

    /**
     * Like get(), but for the default entry layout returns one shared entry object that is overwritten on the next call.
     * This avoids an allocation per entry for callers that only read the fields right away.
     *
     * @param {number} index The 0-based entry number.
     * @returns {EntryInterface|false}
     */
    peek(index) {
        if (!this.wordsPerEntry) {
            return this.get(index);
        }
        const chunk = this.chunkFor(index);
        if (!chunk) {
            return false;
        }
        const word = (index % CHUNK_CAPACITY) * this.wordsPerEntry;
        const words = chunk.words;
        const entry = this.sharedEntry || (this.sharedEntry = new this.EntryClass(0, 0));
        entry[0] = words[word];
        entry[1] = words[word + 1];
        entry[2] = words[word + 2];
        entry[3] = words[word + 3];
        return entry;
    }

    /**
     * Get the sequence number of an entry. Avoids creating an entry object for the default entry layout.
     *
     * @param {number} index The 0-based entry number.
     * @returns {number}
     */
    numberAt(index) {
        if (this.wordsPerEntry) {
            const chunk = this.chunkFor(index);
            return chunk ? chunk.words[(index % CHUNK_CAPACITY) * this.wordsPerEntry] : NaN;
        }
        const entry = this.get(index);
        return entry ? entry.number : NaN;
    }

    /**
     * @param {number} from The 0-based entry number to start at (inclusive).
     * @param {number} until The 0-based entry number to end at (exclusive).
     * @returns {Array<EntryInterface>}
     */
    slice(from, until) {
        const entries = new Array(until - from);
        for (let index = from; index < until; index++) {
            entries[index - from] = this.get(index);
        }
        return entries;
    }

    /**
     * Append an entry at the end.
     *
     * @param {EntryInterface} entry
     */
    push(entry) {
        const chunkIndex = Math.floor(this.length / CHUNK_CAPACITY);
        const position = this.length - chunkIndex * CHUNK_CAPACITY;
        let chunk = position > 0 ? this.chunkFor(this.length - 1) : new Chunk(this.entrySize, MIN_CHUNK_ALLOCATION);
        if (!chunk) {
            throw new Error('Can not append to the index, because its last entries could not be read.');
        }
        if (chunk.capacity === position) {
            chunk = chunk.grow(this.entrySize, allocationFor(position + 1));
        }
        this.chunks[chunkIndex] = chunk;
        entry.toBuffer(chunk.buffer, position * this.entrySize);
        chunk.filled = position + 1;
        this.length++;
    }
}

export default EntryChunks;
export { CHUNK_CAPACITY };
