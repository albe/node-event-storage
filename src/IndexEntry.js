
/**
 * This is the interface that an Entry class needs to implement.
 * @interface
 */
class EntryInterface {
    static get size() {}
    static fromBuffer(buffer, offset = 0) {}
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
     * @param {number} size The size of the stored document (for verification).
     * @param {number} partition The partition where the indexed document is stored.
     */
    constructor(number, position, size = 0, partition = 0) {
        super(4);
        this[0] = number;
        this[1] = position;
        this[2] = size;
        this[3] = partition;
    }

    static get size() {
        return 4 * 4;
    }

    static fromBuffer(buffer, offset = 0) {
        const number     = buffer.readUInt32LE(offset, true);
        const position   = buffer.readUInt32LE(offset +  4, true);
        const size       = buffer.readUInt32LE(offset +  8, true);
        const partition  = buffer.readUInt32LE(offset + 12, true);
        return new this(number, position, size, partition);
    }

    toBuffer(buffer, offset) {
        buffer.writeUInt32LE(this[0], offset, true);
        buffer.writeUInt32LE(this[1], offset +  4, true);
        buffer.writeUInt32LE(this[2], offset +  8, true);
        buffer.writeUInt32LE(this[3], offset + 12, true);
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
