/**
 * A fixed-capacity ring buffer used as an index entry cache.
 *
 * Only the most-recent `capacity` entries are kept in memory.  The buffer also
 * tracks the total number of entries ever added (`length`) so callers can tell
 * whether a slot is within the live in-memory window.
 *
 * API contract
 * ------------
 * - `get(index)`        — cached item at 0-based `index`, or `null` when the
 *                         index is outside the in-memory window or the slot has
 *                         not been written yet.
 * - `set(index, item)`  — stores `item` at 0-based `index` if it falls inside
 *                         the current window; silently ignores out-of-window
 *                         writes.
 * - `add(item)`         — appends `item` at position `length`, advances
 *                         `length`, and returns the new length.
 * - `truncate(newLength)` — discards entries from `newLength` onwards (nulls
 *                         their slots) and sets `length = newLength`.  Safe to
 *                         call with `newLength >= length` (grow-only update).
 * - `reset()`           — clears all slots and resets `length` to 0.
 */
class RingBuffer {

    /**
     * @param {number} capacity  Maximum number of entries held in memory.
     */
    constructor(capacity) {
        this._capacity = Math.max(1, capacity >>> 0); // jshint ignore:line
        this._buffer = new Array(this._capacity);
        this._length = 0;
    }

    /**
     * Total number of items ever appended (not capped at capacity).
     * @type {number}
     */
    get length() {
        return this._length;
    }

    /**
     * Maximum number of items kept in memory.
     * @type {number}
     */
    get capacity() {
        return this._capacity;
    }

    /**
     * The smallest 0-based index that is currently inside the in-memory window.
     * Indices below this value are not cached and require a disk read.
     * @type {number}
     */
    get windowStart() {
        return Math.max(0, this._length - this._capacity);
    }

    /**
     * Return the cached item at the given 0-based index, or `null` if the
     * index is outside the in-memory window or the slot has not been populated.
     *
     * @param {number} index  0-based position.
     * @returns {*|null}
     */
    get(index) {
        if (index < this.windowStart) {
            return null;
        }
        const item = this._buffer[index % this._capacity];
        return item !== undefined ? item : null;
    }

    /**
     * Store `item` at the given 0-based `index`.
     * Writes outside the current in-memory window are silently ignored.
     *
     * @param {number} index  0-based position.
     * @param {*}      item
     */
    set(index, item) {
        if (index < this.windowStart) {
            return;
        }
        this._buffer[index % this._capacity] = item;
    }

    /**
     * Append `item` at position `length` and advance `length`.
     *
     * @param {*} item
     * @returns {number} The new length (1-based position of the appended item).
     */
    add(item) {
        this._buffer[this._length % this._capacity] = item;
        this._length++;
        return this._length;
    }

    /**
     * Discard entries from `newLength` onwards by nulling their cache slots,
     * then set `length = newLength`.
     *
     * When `newLength >= length` no eviction is performed and only `length` is
     * updated (useful when the underlying file has grown and the caller just
     * needs to advance the length counter without populating new slots).
     *
     * @param {number} newLength  The new total length.
     */
    truncate(newLength) {
        if (newLength < this._length) {
            const cacheStart = this.windowStart;
            for (let i = Math.max(cacheStart, newLength); i < this._length; i++) {
                this._buffer[i % this._capacity] = null;
            }
        }
        this._length = newLength;
    }

    /**
     * Return a copy of the cached items for the 0-based range [from, until] (inclusive).
     * Both `from` and `until` must be within the current window (>= windowStart).
     *
     * When the range is contiguous in the internal buffer a single native slice is
     * returned.  When it wraps the two halves are concatenated.
     *
     * @param {number} from   0-based start position (inclusive).
     * @param {number} until  0-based end position (inclusive).
     * @returns {Array<*>}
     */
    slice(from, until) {
        const slotFrom = from % this._capacity;
        const slotUntil = until % this._capacity;
        if (slotFrom <= slotUntil) {
            return this._buffer.slice(slotFrom, slotUntil + 1);
        }
        return this._buffer.slice(slotFrom).concat(this._buffer.slice(0, slotUntil + 1));
    }

    /**
     * Clear all cached slots and reset `length` to 0.
     */
    reset() {
        this._buffer.fill(null);
        this._length = 0;
    }
}

module.exports = RingBuffer;
