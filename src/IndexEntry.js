
/**
 * This is the interface that an Entry class needs to implement.
 * @interface
 */
class EntryInterface {
    /**
     * @return {number} The byte size of this Entry.
     */
    static get size() {}

    /**
     * Read a new Entry from a Buffer object at the given offset.
     *
     * @param {Buffer} buffer The buffer object to read the index data from. 
     * @param {number} [offset] The buffer offset to start reading from. Default 0.
     * @return {EntryInterface} A new entry matching the values from the Buffer.
     */
    static fromBuffer(buffer, offset = 0) {}

    /**
     * Write this Entry into a Buffer object at the given offset.
     *
     * @param {Buffer} buffer The buffer object to write the index entry data to.
     * @param {number} offset The offset to start writing into the buffer.
     * @return {number} The size of the data written.
     */
    toBuffer(buffer, offset) {}
}

/**
 * Assert that the given class is a valid EntryInterface class.
 *
 * @param {function} EntryClass The constructor for the class
 * @throws {Error} if the given class does not implement the EntryInterface methods or has non-positive size
 */
function assertValidEntryClass(EntryClass) {
    if (typeof EntryClass !== 'function'
        || typeof EntryClass.size === 'undefined'
        || typeof EntryClass.fromBuffer !== 'function'
        || typeof EntryClass.prototype.toBuffer !== 'function') {
        throw new Error('Invalid index entry class. Must implement EntryInterface methods.');
    }
    if (EntryClass.size < 1) {
        throw new Error('Entry class size must be positive.');
    }
}

/**
 * Default Entry item contains information about the sequence number, the file position, the document size and the partition number.
 */
class Entry extends Array {

    /**
     * @param {number} number The sequence number of the index entry.
     * @param {number} position The file position where the indexed document is stored.
     * @param {number} [size] The size of the stored document (for verification). Default 0.
     * @param {number} [partition] The partition where the indexed document is stored. Default 0.
     */
    constructor(number, position, size = 0, partition = 0) {
        super(4);
        this[0] = number;
        this[1] = position;
        this[2] = size;
        this[3] = partition;
    }

    /**
     * @return {number} The byte size of this Entry. Always 16.
     */
    static get size() {
        return 4 * 4;
    }

    /**
     * Read a new Entry from a Buffer object at the given offset.
     *
     * @param {Buffer} buffer The buffer object to read the index data from. Will read four 32-Bit LE unsigned integers. 
     * @param {number} [offset] The buffer offset to start reading from. Default 0.
     * @return {Entry} A new entry matching the values from the Buffer.
     */
    static fromBuffer(buffer, offset = 0) {
        const number     = buffer.readUInt32LE(offset);
        const position   = buffer.readUInt32LE(offset +  4);
        const size       = buffer.readUInt32LE(offset +  8);
        const partition  = buffer.readUInt32LE(offset + 12);
        return new this(number, position, size, partition);
    }

    /**
     * Write this Entry into a Buffer object at the given offset.
     *
     * @param {Buffer} buffer The buffer object to write the index entry data to. Will write four 32-Bit LE unsigned integers.
     * @param {number} offset The offset to start writing into the buffer.
     * @return {number} The size of the data written. Will always be 16.
     */
    toBuffer(buffer, offset) {
        buffer.writeUInt32LE(this[0], offset);
        buffer.writeUInt32LE(this[1], offset +  4);
        buffer.writeUInt32LE(this[2], offset +  8);
        buffer.writeUInt32LE(this[3], offset + 12);
        return Entry.size;
    }

    get number() {
        return this[0];
    }

    get position() {
        return this[1];
    }

    get size() {
        return this[2];
    }

    get partition() {
        return this[3];
    }

}

module.exports = Entry;
module.exports.EntryInterface = EntryInterface;
module.exports.assertValidEntryClass = assertValidEntryClass;
