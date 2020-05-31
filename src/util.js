const crypto = require('crypto');

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
 * @param {string} secret The secret to use for calculating further HMACs
 * @returns {function(string)} A function that calculates the HMAC for a given string
 */
const createHmac = secret => string => {
        const hmac = crypto.createHmac('sha256', secret);
        hmac.update(string);
        return hmac.digest('hex');
    };

/**
 * @param {Object} document The document to check against the matcher.
 * @param {Object|function} matcher An object of properties and their values that need to match in the object or a function that checks if the document matches.
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
        } else if (document[prop] !== matcher[prop]) {
            return false;
        }
    }
    return true;
}

/**
 * @param {Object|function} matcher The matcher object or function that should be serialized.
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
 * @param {{matcher: string|Object, hmac: string}} matcherMetadata The serialized matcher and it's HMAC
 * @param {function(string)} hmac A function that calculates a HMAC of the given string.
 * @returns {Object|function} The matcher object or function.
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
 * @returns {number|boolean} The wrapped index or false if index out of bounds.
 */
function wrapAndCheck(index, length) {
    if (typeof index !== 'number') {
        return false;
    }

    if (index < 0) {
        index += length + 1;
    }
    if (index < 1 || index > length) {
        return false;
    }
    return index;
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
    buildMetadataHeader
};