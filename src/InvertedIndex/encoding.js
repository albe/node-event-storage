const TWO_POW_31 = 0x80000000;

/**
 * A growable byte buffer for sequentially encoding varints and raw bytes.
 */
class ByteWriter {

    constructor(initialSize = 4096) {
        this.buffer = Buffer.allocUnsafe(initialSize);
        this.length = 0;
    }

    ensure(additional) {
        const required = this.length + additional;
        if (required <= this.buffer.length) {
            return;
        }
        let size = this.buffer.length * 2;
        while (size < required) size *= 2;
        const grown = Buffer.allocUnsafe(size);
        this.buffer.copy(grown, 0, 0, this.length);
        this.buffer = grown;
    }

    /**
     * Unsigned LEB128 for integers up to 2^53. Values below 2^31 take the bit-shift fast path.
     *
     * @param {number} value
     */
    varint(value) {
        this.ensure(8);
        const buffer = this.buffer;
        let cursor = this.length;
        while (value >= TWO_POW_31) {
            buffer[cursor++] = (value % 128) | 128;
            value = Math.floor(value / 128);
        }
        while (value >= 128) {
            buffer[cursor++] = (value & 127) | 128;
            value >>>= 7;
        }
        buffer[cursor++] = value;
        this.length = cursor;
    }

    bytes(source, start = 0, end = source.length) {
        this.ensure(end - start);
        source.copy(this.buffer, this.length, start, end);
        this.length += end - start;
    }

    doubleLE(value) {
        this.ensure(8);
        this.buffer.writeDoubleLE(value, this.length);
        this.length += 8;
    }

    uint32LE(value) {
        this.ensure(4);
        this.buffer.writeUInt32LE(value >>> 0, this.length);
        this.length += 4;
    }

    toBuffer() {
        return this.buffer.subarray(0, this.length);
    }
}

/**
 * Sequential decoder over a buffer region.
 */
class ByteReader {

    constructor(buffer, offset = 0) {
        this.buffer = buffer;
        this.offset = offset;
    }

    varint() {
        const buffer = this.buffer;
        let cursor = this.offset;
        let byte = buffer[cursor++];
        let value = byte & 127;
        let shift = 7;
        while (byte & 128) {
            byte = buffer[cursor++];
            value += (byte & 127) * 2 ** shift;
            shift += 7;
        }
        this.offset = cursor;
        return value;
    }

    bytes(length) {
        const slice = this.buffer.subarray(this.offset, this.offset + length);
        this.offset += length;
        return slice;
    }
}

export { ByteWriter, ByteReader };
