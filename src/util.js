const crypto = require('crypto');

/**
 * @param {string} secret The secret to use for calculating further HMACs
 * @returns {Function(string)} A function that calculates the HMAC for a given string
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
 * @param {Function(string)} hmac A function that calculates a HMAC of the given string.
 * @returns {{matcher: string, hmac: string}}
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
 * @param {{matcher: string, hmac: string}} matcherMetadata The serialized matcher and it's HMAC
 * @param {Function(string)} hmac A function that calculates a HMAC of the given string.
 * @returns {Object|function}The matcher object or function.
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
 * Assert that actual and expected match or throw an Error with the given message.
 *
 * @param {*} actual
 * @param {*} expected
 * @param {string} message
 */
function assertEqual(actual, expected, message) {
    if (actual !== expected) {
        throw new Error(message + `, got ${actual}, expected ${expected}.`);
    }
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

module.exports = { assertEqual, wrapAndCheck, binarySearch, createHmac, matches, buildMetadataForMatcher, buildMatcherFromMetadata };