/**
 * Assert that actual and expected match or throw an Error with the given message appended by information about expected and actual value.
 *
 * @param {*} actual
 * @param {*} expected
 * @param {string} message
 */
function assertEqual(actual, expected, message) {
    if (actual !== expected) {
        throw new Error(message + (message ? ' ' : '') + `Expected "${expected}" but got "${actual}".`);
    }
}

/**
 * Assert that the condition holds and if not, throw an error with the given message.
 *
 * @param {boolean} condition
 * @param {string} message
 * @param {typeof Error} ErrorType
 */
function assert(condition, message, ErrorType = Error) {
    if (!condition) {
        throw new ErrorType(message);
    }
}

/**
 * Return the amount required to align value to the given alignment.
 * It calculates the difference of the alignment and the modulo of value by alignment.
 * @param {number} value
 * @param {number} alignment
 * @returns {number}
 */
function alignTo(value, alignment) {
    return (alignment - (value % alignment)) % alignment;
}

/**
 * Method for hashing a string (e.g. a partition name) to a 32-bit unsigned integer.
 *
 * @param {string} str
 * @returns {number}
 */
function hash(str) {
    /* istanbul ignore if */
    if (str.length === 0) {
        return 0;
    }
    let hash = 5381,
        i    = str.length;

    while(i) {
        hash = ((hash << 5) + hash) ^ str.charCodeAt(--i); // jshint ignore:line
    }

    /* JavaScript does bitwise operations (like XOR, above) on 32-bit signed
     * integers. Since we want the results to be always positive, convert the
     * signed int to an unsigned by doing an unsigned bitshift. */
    return hash >>> 0; // jshint ignore:line
}

/**
 * Do a binary search for number in the range 1-length with values retrieved via a provided getter.
 *
 * @param {number} number The value to search for
 * @param {number} length The upper position to search up to
 * @param {function(number)} get The getter function to retrieve the values at the specific position
 * @returns {Array<number>} An array of the low and high position that match the searched number
 */
function binarySearch(number, length, get) {
    let low = 1;
    let high = length;

    if (get(low) > number) {
        return [low, 0];
    }
    if (get(high) < number) {
        return [0, high];
    }

    while (low <= high) {
        const mid = low + ((high - low) >> 1);
        const value = get(mid);
        if (value === number) {
            return [mid, mid];
        }
        if (value < number) {
            low = mid + 1;
        } else {
            high = mid - 1;
        }
    }
    return [low, high];
}

/**
 * @param {number} index The 1-based index position to wrap around if < 0 and check against the bounds.
 * @param {number} length The length of the index and upper bound.
 * @returns {number} The wrapped index position or -1 if index out of bounds.
 */
function wrapAndCheck(index, length) {
    if (typeof index !== 'number') {
        return -1;
    }

    if (index < 0) {
        index += length + 1;
    }
    if (index < 1 || index > length) {
        return -1;
    }
    return index;
}

/**
 * Iterate an array-like list in forward or reverse order.
 *
 * @param {Iterable} entries
 * @param {boolean} forwards
 */
function* iterate(entries, forwards) {
    if (forwards) {
        yield* entries;
        return;
    }

    for (let i = entries.length - 1; i >= 0; i--) {
        yield entries[i];
    }
}

/**
 * Perform a k-way merge over multiple iterables in sort-key order.
 *
 * Each iterable is primed by calling `.next()` once at startup. On each merge step the iterable
 * with the best current value is advanced and its value is yielded (after passing through `visit`).
 * An iterable is dropped once its iterator reports `done`.
 *
 * @param {Iterable[]|Iterator[]} iterables Iterables or bare iterators to merge.
 * @param {function(*): number} getSortKey Extracts the numeric sort key from an iterable's current value.
 * @param {boolean} [ascending=true] When true, yields items in ascending key order (min-merge).
 *   When false, yields in descending key order (max-merge).
 * @param {function(*): *} [visit] Optional extractor for the yielded value. Defaults to identity.
 * @returns {Generator<*>}
 */
function *kWayMerge(iterables, getSortKey, ascending = true, visit = v => v) {
    const states = [];
    for (const iterable of iterables) {
        const iterator = typeof iterable[Symbol.iterator] === 'function' ? iterable[Symbol.iterator]() : iterable;
        const { value, done } = iterator.next();
        if (!done) {
            states.push({ iterator, current: value });
        }
    }

    while (states.length > 0) {
        let bestIdx = 0;
        for (let i = 1; i < states.length; i++) {
            const better = ascending
                ? getSortKey(states[i].current) < getSortKey(states[bestIdx].current)
                : getSortKey(states[i].current) > getSortKey(states[bestIdx].current);
            if (better) bestIdx = i;
        }
        yield visit(states[bestIdx].current);
        const { value, done } = states[bestIdx].iterator.next();
        if (done) {
            states.splice(bestIdx, 1);
        } else {
            states[bestIdx].current = value;
        }
    }
}

/**
 * Read a scalar value at a dot-notation path from an object.
 * Returns `undefined` if any path segment is absent or an intermediate value is not an object.
 *
 * @param {object} obj
 * @param {string} dotPath Dot-separated property path, e.g. `'payload.type'`.
 * @returns {*}
 */
function getPropertyAtPath(obj, dotPath) {
    let current = obj;
    const parts = dotPath.split('.');
    for (const part of parts) {
        if (current == null || typeof current !== 'object') return undefined;
        current = current[part];
    }
    return current;
}

export {
    assert,
    assertEqual,
    hash,
    wrapAndCheck,
    iterate,
    binarySearch,
    alignTo,
    kWayMerge,
    getPropertyAtPath
};