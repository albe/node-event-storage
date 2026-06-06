import crypto from 'crypto';
import {assert, assertEqual} from './util.js';
import {
    indexOfSameLevel,
    findJsonValueEnd,
    parseJsonValue,
    matchesAnyValuePattern,
    isOpeningObject
} from './jsonUtil.js';

const compiledOperatorMatcherCache = new WeakMap();

/**
 * @param {any} value
 * @returns {boolean}
 */
function isObject(value) {
    return value !== null && typeof value === 'object';
}

/**
 * @param {any} value
 * @returns {boolean}
 */
function isPlainObject(value) {
    return value !== null && typeof value === 'object' && !Array.isArray(value);
}

/**
 * @param {object} obj
 * @returns {boolean}
 */
function isOperatorObject(obj) {
    const keys = Object.keys(obj);
    return keys.length > 0 && keys.every(key => key.startsWith('$'));
}

/**
 * Dispatch between array (OR), operator object, nested object, and scalar equality matching
 * so callers don't need to know the shape of `matcherValue`.
 *
 * @param {any} documentValue
 * @param {any} matcherValue
 * @returns {boolean}
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
 *
 * @param {object} operatorObj
 * @returns {Array<function(any): boolean>}
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
 *
 * @param {object} operatorObj
 * @returns {Array<function(any): boolean>}
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

/**
 * @param {any} documentValue
 * @param {Array<function(any): boolean>} checks
 * @returns {boolean}
 */
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
        return {matcher};
    }
    const matcherString = matcher.toString();
    return {matcher: matcherString, hmac: hmac(matcherString)};
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
    return function (typeValue) {
        let obj = typeValue;
        for (let i = parts.length - 1; i >= 0; i--) {
            obj = {[parts[i]]: obj};
        }
        return {payload: obj};
    };
}

/**
 * Compile an object matcher into a raw-buffer predicate so raw-mode reads can filter compact
 * JSON without parsing every document first.
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
        if (!isOpeningObject(buffer[0])) {
            return false;
        }
        if (!preCheck(buffer, 1, root)) {
            return false;
        }
        return matchesNode(buffer, 1, root);
    };
}

/**
 * Compile a matcher object into a tree whose children each carry one primary byte pattern plus
 * optional follow-up checks for nested objects, operators, or multi-value scalars.
 *
 * @param {object} matcher
 * @returns {{children: Array<object>}}
 */
function buildMatcherTree(matcher) {
    const node = {children: []};

    for (const [key, value] of Object.entries(matcher)) {
        node.children.push(buildMatcherTreeChild(key, value));
    }

    return node;
}

/**
 * Normalize one matcher property into the cheapest raw-buffer strategy for that value shape.
 *
 * @param {string} key
 * @param {any} value
 * @returns {{pattern: Buffer, isKeyPattern: boolean, operatorChecks: (Array<function(any): boolean>|null), valuePatterns: (Buffer[]|null), node: ({children: Array<object>}|null), lastMatch: number}}
 */
function buildMatcherTreeChild(key, value) {
    const keyPrefix = Buffer.from(`${JSON.stringify(key)}:`, 'utf8');
    const child = {
        pattern: null,
        isKeyPattern: false,
        operatorChecks: null,
        valuePatterns: null,
        node: null,
        lastMatch: -1
    };

    if (isObject(value)) {
        if (Array.isArray(value)) {
            assert(!value.some(isObject), 'Array matcher values must be scalars.', TypeError);
            if (value.length === 1) {
                child.pattern = buildKeyValuePattern(keyPrefix, value[0]);
            } else {
                child.isKeyPattern = true;
                child.pattern = keyPrefix;
                child.valuePatterns = value.map(item => Buffer.from(JSON.stringify(item), 'utf8'));
            }
        } else if ('$eq' in value && Object.keys(value).length === 1) {
            // A lone $eq is semantically identical to a scalar equality check — fold it into a
            // value pattern at compile time so the buffer scan takes the same fast path as { key: value }.
            child.pattern = buildKeyValuePattern(keyPrefix, value['$eq']);
        } else if (isOperatorObject(value)) {
            child.operatorChecks = getCompiledOperatorChecks(value);
            child.isKeyPattern = true;
            child.pattern = keyPrefix;
        } else {
            child.pattern = Buffer.concat([keyPrefix, Buffer.from('{', 'utf8')]);
            child.node = buildMatcherTree(value);
        }
    } else {
        child.pattern = buildKeyValuePattern(keyPrefix, value);
    }
    return child;
}

/**
 * @param {Buffer} keyPrefix
 * @param {any} value
 * @returns {Buffer}
 */
function buildKeyValuePattern(keyPrefix, value) {
    return Buffer.concat([keyPrefix, Buffer.from(JSON.stringify(value), 'utf8')]);
}

/**
 * Cheap pass: confirm each child's primary pattern exists somewhere and cache that position as a
 * hint for the depth-aware pass that follows.
 *
 * @param {Buffer} buffer
 * @param {number} startOffset
 * @param {{children: Array<object>}} node
 * @returns {boolean}
 */
function preCheck(buffer, startOffset, node) {
    for (const child of node.children) {
        child.lastMatch = buffer.indexOf(child.pattern, startOffset);
        if (child.lastMatch === -1) {
            return false;
        }
        if (child.node && !preCheck(buffer, child.lastMatch, child.node)) {
            return false;
        }
    }
    return true;
}

/**
 * Confirm that each prechecked child really matches at the requested JSON level and then run the
 * optional value-specific follow-up checks from the compiled tree.
 *
 * @param {Buffer} buffer
 * @param {number} startOffset
 * @param {{children: Array<object>}} node
 * @returns {boolean}
 */
function matchesNode(buffer, startOffset, node) {
    for (const child of node.children) {
        const matchPosition = indexOfSameLevel(buffer, child.pattern, startOffset, child.lastMatch, child.isKeyPattern);
        if (matchPosition === -1) {
            return false;
        }

        const valueStart = matchPosition + child.pattern.length;
        if (child.valuePatterns && !matchesAnyValuePattern(buffer, valueStart, child.valuePatterns)) {
            return false;
        }
        if (child.operatorChecks && !matchesOperatorInBuffer(buffer, valueStart, child.operatorChecks)) {
            return false;
        }
        if (child.node && !matchesNode(buffer, valueStart, child.node)) {
            return false;
        }
    }

    return true;
}

/**
 * Parse the scalar at a matched key position only when operator objects require a real JavaScript
 * comparison instead of a pure byte-pattern check.
 *
 * @param {Buffer} buffer
 * @param {number} startOffset
 * @param {Array<function(any): boolean>} operatorChecks
 * @returns {boolean}
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
