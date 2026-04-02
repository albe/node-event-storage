import crypto from 'crypto';

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
 * @param {{matcher: string|object, hmac: string}} matcherMetadata The serialized matcher and its HMAC
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

export {
    createHmac,
    matches,
    buildMetadataForMatcher,
    buildMatcherFromMetadata
};
