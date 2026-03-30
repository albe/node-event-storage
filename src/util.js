const crypto = require('crypto');
const fs = require('fs');
const mkdirpSync = require('mkdirp').sync;

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
 * @param {string} secret The secret to use for calculating further HMACs
 * @returns {function(string)} A function that calculates the HMAC for a given string
 */
const createHmac = secret => string => {
        const hmac = crypto.createHmac('sha256', secret);
        hmac.update(string);
        return hmac.digest('hex');
    };

/**
 * @typedef {object|function(object):boolean} Matcher
 */

/**
 * @param {object} document The document to check against the matcher.
 * @param {Matcher} matcher An object of properties and their values that need to match in the object or a function that checks if the document matches.
 * @returns {boolean} True if the document matches the matcher or false otherwise.
 */
function matches(document, matcher) {
    if (typeof document === 'undefined') return false;
    if (typeof matcher === 'undefined') return true;

    if (typeof matcher === 'function') return matcher(document);

    for (let prop of Object.getOwnPropertyNames(matcher)) {
        if (typeof matcher[prop] === 'object') {
            if (!matches(document[prop], matcher[prop])) {
                return false;
            }
        } else if (typeof matcher[prop] !== 'undefined' && document[prop] !== matcher[prop]) {
            return false;
        }
    }
    return true;
}

/**
 * @param {Matcher} matcher The matcher object or function that should be serialized.
 * @param {function(string)} hmac A function that calculates a HMAC of the given string.
 * @returns {{matcher: string|object, hmac?: string}}
 */
function buildMetadataForMatcher(matcher, hmac) {
    if (!matcher) {
        return undefined;
    }
    if (typeof matcher === 'object') {
        return { matcher };
    }
    const matcherString = matcher.toString();
    return { matcher: matcherString, hmac: hmac(matcherString) };
}

/**
 * @param {{matcher: string|object, hmac: string}} matcherMetadata The serialized matcher and it's HMAC
 * @param {function(string)} hmac A function that calculates a HMAC of the given string.
 * @returns {Matcher} The matcher object or function.
 */
function buildMatcherFromMetadata(matcherMetadata, hmac) {
    let matcher;
    if (typeof matcherMetadata.matcher === 'object') {
        matcher = matcherMetadata.matcher;
    } else {
        if (matcherMetadata.hmac !== hmac(matcherMetadata.matcher)) {
            throw new Error('Invalid HMAC for matcher.');
        }
        matcher = eval('(' + matcherMetadata.matcher + ')').bind({}); // jshint ignore:line
    }
    return matcher;
}

/**
 * Build a buffer containing the file magic header and a JSON stringified metadata block, padded to be a multiple of 16 bytes long.
 *
 * @param {string} magic
 * @param {object} metadata
 * @returns {Buffer} A buffer containing the header data
 */
function buildMetadataHeader(magic, metadata) {
    assertEqual(magic.length, 8, 'The header magic bytes length is wrong.');
    let metadataString = JSON.stringify(metadata);
    let metadataSize = Buffer.byteLength(metadataString, 'utf8');
    // 8 byte MAGIC, 4 byte metadata size, 1 byte line break
    const pad = (16 - ((8 + 4 + metadataSize + 1) % 16)) % 16;
    metadataString += ' '.repeat(pad) + "\n";
    metadataSize += pad + 1;
    const metadataBuffer = Buffer.allocUnsafe(8 + 4 + metadataSize);
    metadataBuffer.write(magic, 0, 8, 'utf8');
    metadataBuffer.writeUInt32BE(metadataSize, 8);
    metadataBuffer.write(metadataString, 8 + 4, metadataSize, 'utf8');
    return metadataBuffer;
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
 * Ensure that the given directory exists.
 * @param {string} dirName
 * @return {boolean} true if the directory existed already
 */
function ensureDirectory(dirName) {
    if (!fs.existsSync(dirName)) {
        try {
            mkdirpSync(dirName);
        } catch (e) {
        }
        return false;
    }
    return true;
}

/**
 * Perform a k-way merge over multiple streams, invoking a callback for each item in ascending key order.
 * Each stream object is mutated in place by the `advance` function.
 *
 * @param {object[]} streams Array of stream state objects; entries are removed when exhausted.
 * @param {function(object): number} getKey Returns the current sort key for a stream state.
 * @param {function(object): boolean} advance Advances the stream to its next item.
 *   Returns true if the stream has more items within range, false if exhausted.
 * @param {function(object): void} visit Called for each stream state in merged order.
 */
function kWayMerge(streams, getKey, advance, visit) {
    while (streams.length > 0) {
        let minIdx = 0;
        for (let i = 1; i < streams.length; i++) {
            if (getKey(streams[i]) < getKey(streams[minIdx])) {
                minIdx = i;
            }
        }
        visit(streams[minIdx]);
        if (!advance(streams[minIdx])) {
            streams.splice(minIdx, 1);
        }
    }
}

/**
 * Scan a directory for files whose names match a regex pattern, calling a callback for each match.
 * The `onEach` callback receives the first capturing group of the match (`match[1]`), or the full
 * match (`match[0]`) when no capturing group is defined in the pattern.
 *
 * @param {string} directory The directory to scan.
 * @param {RegExp} regexPattern The pattern to match file names against.
 * @param {function(string)} onEach Called with the first capturing group (or full match) for each matching file name.
 * @param {function(Error?)} onDone Called when the scan is complete, or with an error if one occurred.
 */
function scanForFiles(directory, regexPattern, onEach, onDone) {
    fs.readdir(directory, (err, files) => {
        if (err) {
            return onDone(err);
        }
        let match;
        for (let file of files) {
            if ((match = file.match(regexPattern)) !== null) {
                onEach(match[1] !== undefined ? match[1] : match[0]);
            }
        }
        onDone(null);
    });
}


module.exports = {
    assert,
    assertEqual,
    wrapAndCheck,
    binarySearch,
    createHmac,
    matches,
    buildMetadataForMatcher,
    buildMatcherFromMetadata,
    buildMetadataHeader,
    alignTo,
    ensureDirectory,
    scanForFiles,
    kWayMerge
};