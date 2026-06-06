import crypto from 'crypto';
import { assert, assertEqual } from './util.js';
import { BYTE_OPEN_OBJECT, indexOfSameLevel, findJsonValueEnd, parseJsonValue } from './jsonUtil.js';

const compiledOperatorMatcherCache = new WeakMap();

function isObject(value) {
    return value !== null && typeof value === 'object';
}

function isPlainObject(value) {
    return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function isOperatorObject(obj) {
    const keys = Object.keys(obj);
    return keys.length > 0 && keys.every(key => key.startsWith('$'));
}

/**
 * Dispatch between array (OR), operator object, nested object, and scalar equality matching
 * so callers don't need to know the shape of `matcherValue`.
 */
function propertyMatchesValue(documentValue, matcherValue) {
    if (isObject(matcherValue)) {
        if (Array.isArray(matcherValue)) {
            return matcherValue.includes(documentValue);
        } else if (isOperatorObject(matcherValue)) {
            const operatorChecks = getCompiledOperatorChecks(matcherValue);
            return matchesCompiledOperators(documentValue, operatorChecks);
        }
        return matches(documentValue, matcherValue);
    }
    return typeof matcherValue === 'undefined' || documentValue === matcherValue;
}

/**
 * Pre-compile an operator object into an array of comparison closures so the hot path avoids
 * repeated `Object.entries` + switch dispatch per matched document.
 */
function buildOperatorChecks(operatorObj) {
    const checks = [];
    for (const [operator, expectedValue] of Object.entries(operatorObj)) {
        switch (operator) {
            case '$gt':
                checks.push(value => value > expectedValue);
                break;
            case '$gte':
                checks.push(value => value >= expectedValue);
                break;
            case '$lt':
                checks.push(value => value < expectedValue);
                break;
            case '$lte':
                checks.push(value => value <= expectedValue);
                break;
            case '$eq':
                checks.push(value => value === expectedValue);
                break;
            case '$ne':
                checks.push(value => value !== expectedValue);
                break;
            default:
                throw new TypeError(`Unknown operator: ${operator}`);
        }
    }
    return checks;
}

/**
 * Return cached compiled checks for `operatorObj`, compiling and caching on first access.
 * Using WeakMap keeps operator objects GC-eligible and avoids mutating user-supplied objects.
 */
function getCompiledOperatorChecks(operatorObj) {
    const cachedChecks = compiledOperatorMatcherCache.get(operatorObj);
    if (cachedChecks) {
        return cachedChecks;
    }
    const checks = buildOperatorChecks(operatorObj);
    compiledOperatorMatcherCache.set(operatorObj, checks);
    return checks;
}

function matchesCompiledOperators(documentValue, checks) {
    if (documentValue === undefined) {
        return false;
    }
    for (const check of checks) {
        if (!check(documentValue)) {
            return false;
        }
    }
    return true;
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
        if (!propertyMatchesValue(document[prop], matcher[prop])) {
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
    /* c8 ignore next */
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
        /* c8 ignore next */
        assert(matcherMetadata.hmac === hmac(matcherMetadata.matcher), 'Invalid HMAC for matcher.');

        matcher = eval('(' + matcherMetadata.matcher + ')').bind({}); // jshint ignore:line
    }
    return matcher;
}

/**
 * Builds a factory function that, given a type string, returns an object matcher for
 * documents whose payload contains that type at the given dot-notation path.
 *
 * @param {string} payloadPath Dot-notation path relative to the event payload (e.g. `'type'`, `'meta.kind'`).
 * @returns {function(string): object} A function `(typeValue) => objectMatcher`.
 */
function buildTypeMatcherFn(payloadPath) {
    const parts = payloadPath.split('.');
    return function(typeValue) {
        let obj = typeValue;
        for (let i = parts.length - 1; i >= 0; i--) {
            obj = { [parts[i]]: obj };
        }
        return { payload: obj };
    };
}

/**
 * Builds a raw-buffer matcher.
 * It expects the Buffer to contain compact stringified JSON
 * and supports matcher objects with sub properties and multi-value matches (OR/any of).
 *
 * @param {object} matcher Object matcher.
 * @returns {function(Buffer): boolean}
 */
function buildRawBufferMatcher(matcher = {}) {
    assert(isPlainObject(matcher), 'Matcher must be an object.', TypeError);

    const root = buildMatcherTree(matcher);
    /* c8 ignore next 3 */
    if (root.children.length === 0) {
        return () => true;
    }

    return function matchesRawBuffer(buffer) {
        if (buffer[0] !== BYTE_OPEN_OBJECT) {
            return false;
        }
        if (!preCheck(buffer, 1, root)) {
            return false;
        }
        return matchesNode(buffer, 1, root);
    };
}

/**
 * Pre-compile a plain object matcher into a tree of byte patterns so `matchesNode` can scan
 * raw JSON buffers without deserializing them.
 */
function buildMatcherTree(matcher) {
    const node = { children: [] };

    for (const [key, value] of Object.entries(matcher)) {
        node.children.push(buildMatcherTreeChild(key, value));
    }

    return node;
}

/**
 * Build a matcher over a predicate for an array of patterns that checks if any one pattern matches
 */
function matchAny(patterns) {
    return (predicate) => patterns.some(predicate);
}
function matchOne(pattern) {
    return (predicate) => predicate(pattern, 0);
}

/**
 * Decide which matching strategy to use for a key/value pair and build the corresponding
 * child node with pre-computed byte patterns or compiled operator checks.
 */
function buildMatcherTreeChild(key, value) {
    const keyPrefix = Buffer.from(`${JSON.stringify(key)}:`, 'utf8');
    const child = {
        match: () => false,
        isKeyPattern: false,
        operatorChecks: null,
        node: null,
        lastMatches: [] // Cached positions of indexOf patterns
    };

    if (isObject(value)) {
        if (Array.isArray(value)) {
            assert(!value.some(isObject), 'Array matcher values must be scalars.', TypeError);

            child.match = matchAny(value.map(item => buildValuePattern(keyPrefix, item)));
        } else if ('$eq' in value && Object.keys(value).length === 1) {
            // A lone $eq is semantically identical to a scalar equality check — fold it into a
            // value pattern at compile time so the buffer scan takes the same fast path as { key: value }.
            child.match = matchOne(buildValuePattern(keyPrefix, value['$eq']));
        } else if (isOperatorObject(value)) {
            child.operatorChecks = getCompiledOperatorChecks(value);
            child.isKeyPattern = true;
            child.match = matchOne(keyPrefix);
        } else {
            child.match = matchOne(Buffer.concat([keyPrefix, Buffer.from('{', 'utf8')]));
            child.node = buildMatcherTree(value);
        }
    } else {
        child.match = matchOne(buildValuePattern(keyPrefix, value));
    }
    return child;
}

function buildValuePattern(keyPrefix, value) {
    return Buffer.concat([keyPrefix, Buffer.from(JSON.stringify(value), 'utf8')]);
}

function childPatternsExist(buffer, pattern, i, startOffset, child) {
    child.lastMatches[i] = buffer.indexOf(pattern, startOffset);
    if (child.lastMatches[i] === -1) {
        return false;
    }
    return !(child.node && !preCheck(buffer, child.lastMatches[0], child.node));
}

/**
 * Optimization pass: check that every required byte pattern is present anywhere in the buffer
 * before spending the more expensive per-depth scan in `matchesNode`.
 */
function preCheck(buffer, startOffset, node) {
    for (const child of node.children) {
        //child.lastMatches.fill(-1);   // this would be the correct thing to do, but would do an array fill on every level for every check
        if (!child.match((pattern, i) => childPatternsExist(buffer, pattern, i, startOffset, child))) {
            return false;
        }
    }
    return true;
}

function matchesChild(buffer, pattern, startOffset, lastMatchPosition, child) {
    const matchPosition = indexOfSameLevel(buffer, pattern, startOffset, lastMatchPosition, child.isKeyPattern);
    if (matchPosition === -1) {
        return false;
    }
    if (child.operatorChecks && !matchesOperatorInBuffer(buffer, matchPosition + pattern.length, child.operatorChecks)) {
        return false;
    }
    return !child.node || matchesNode(buffer, matchPosition + pattern.length, child.node);
}

/**
 * Verify that each required byte pattern in the tree is present at the correct JSON nesting
 * depth so values inside nested objects don't satisfy a top-level match requirement.
 */
function matchesNode(buffer, startOffset, node) {
    for (const child of node.children) {
        if (!child.match((pattern, i) => matchesChild(buffer, pattern, startOffset, child.lastMatches[i], child))) {
            return false;
        }
    }

    return true;
}

/**
 * Check if a value at the given key-offset matches the specified operators.
 * Finds the key pattern, extracts the value, parses it, and applies operators.
 */
function matchesOperatorInBuffer(buffer, startOffset, operatorChecks) {
    const valueStart = startOffset;
    const valueEnd = findJsonValueEnd(buffer, valueStart);
    if (valueEnd === -1 || valueEnd <= valueStart) {
        return false;
    }

    const parsedValue = parseJsonValue(buffer, valueStart, valueEnd);

    return matchesCompiledOperators(parsedValue, operatorChecks);
}

export {
    createHmac,
    matches,
    buildMetadataHeader,
    buildMetadataForMatcher,
    buildMatcherFromMetadata,
    buildTypeMatcherFn,
    buildRawBufferMatcher
};
