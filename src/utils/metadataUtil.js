import crypto from 'crypto';
import {assert, assertEqual} from './util.js';
import {
    indexOfSameLevel,
    findJsonValueEnd,
    parseJsonValue,
    matchesAnyValuePattern,
    isOpeningObject,
    compareNumeric
} from './jsonUtil.js';

const compiledOperatorMatcherCache = new WeakMap();

/**
 * @param {any} value Value to classify.
 * @returns {boolean} True when `value` is a non-null object.
 */
function isObject(value) {
    return value !== null && typeof value === 'object';
}

/**
 * @param {any} value Value to classify.
 * @returns {boolean} True when `value` is a non-array object.
 */
function isPlainObject(value) {
    return value !== null && typeof value === 'object' && !Array.isArray(value);
}

/**
 * @param {object} obj Candidate matcher object.
 * @returns {boolean} True when all keys are operator keys (`$...`).
 */
function isOperatorObject(obj) {
    const keys = Object.keys(obj);
    return keys.length > 0 && keys.every(key => key.startsWith('$'));
}

/**
 * Dispatch between array (OR), operator object, nested object, and scalar equality matching
 * so callers don't need to know the shape of `matcherValue`.
 *
 * @param {any} documentValue Value from the document.
 * @param {any} matcherValue Value from the matcher definition.
 * @returns {boolean} True when both values match under matcher semantics.
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
 * @param {object} operatorObj Object containing operator/value pairs.
 * @returns {Array<function(any): boolean>} Compiled predicate checks in evaluation order.
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
 * @param {object} operatorObj Object containing operator/value pairs.
 * @returns {Array<function(any): boolean>} Cached or newly compiled operator checks.
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
 * @param {any} documentValue Parsed scalar value from the document.
 * @param {Array<function(any): boolean>} checks Compiled operator checks.
 * @returns {boolean} True when all checks pass.
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
     /* c8 ignore next 2 */
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
         /* c8 ignore next 1 */
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
 * @param {object} matcher Object matcher to compile.
 * @param {{enableOperatorBufferMatcher?: boolean}} [options] Raw matcher build options.
 * @returns {function(Buffer): boolean} Predicate over compact JSON buffers.
 */
function buildRawBufferMatcher(matcher = {}, options = {}) {
    assert(isPlainObject(matcher), 'Matcher must be an object.', TypeError);
    const enableOperatorBufferMatcher = options.enableOperatorBufferMatcher !== false;

    const root = buildMatcherTree(matcher, enableOperatorBufferMatcher);
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
 * @param {object} matcher Matcher object for this tree level.
 * @param {boolean} enableOperatorBufferMatcher Enables specialized byte-level operator shortcuts.
 * @returns {{children: Array<object>}} Compiled child descriptors for this level.
 */
function buildMatcherTree(matcher, enableOperatorBufferMatcher) {
    const node = {children: []};

    for (const [key, value] of Object.entries(matcher)) {
        node.children.push(buildMatcherTreeChild(key, value, enableOperatorBufferMatcher));
    }

    return node;
}

/**
 * Normalize one matcher property into the cheapest raw-buffer strategy for that value shape.
 *
 * @param {string} key Property name at this matcher level.
 * @param {any} value Matcher value for `key`.
 * @param {boolean} enableOperatorBufferMatcher Enables specialized byte-level operator shortcuts.
 * @returns {{pattern: Buffer, isKeyPattern: boolean, matches: ((function(Buffer, number): boolean)|null), node: ({children: Array<object>}|null), lastMatch: number}} Compiled descriptor consumed by preCheck/matchesNode.
 */
function buildMatcherTreeChild(key, value, enableOperatorBufferMatcher) {
    const keyPrefix = Buffer.from(`${JSON.stringify(key)}:`, 'utf8');
    const child = {
        pattern: null,
        isKeyPattern: false,
        matches: null,
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
                const valuePatterns = value.map(item => Buffer.from(JSON.stringify(item), 'utf8'));
                child.matches = (buffer, startOffset) => matchesAnyValuePattern(buffer, startOffset, valuePatterns);
            }
        } else if ('$eq' in value && Object.keys(value).length === 1) {
            // A lone $eq is semantically identical to a scalar equality check — fold it into a
            // value pattern at compile time so the buffer scan takes the same fast path as { key: value }.
            child.pattern = buildKeyValuePattern(keyPrefix, value['$eq']);
        } else if (isOperatorObject(value)) {
            child.isKeyPattern = true;
            child.pattern = keyPrefix;
            if (enableOperatorBufferMatcher) {
                child.matches = buildOperatorBufferMatcher(value);
            } else {
                const operatorChecks = getCompiledOperatorChecks(value);
                child.matches = (buffer, startOffset) => matchesOperatorInBuffer(buffer, startOffset, operatorChecks);
            }
        } else {
            child.pattern = Buffer.concat([keyPrefix, Buffer.from('{', 'utf8')]);
            child.node = buildMatcherTree(value, enableOperatorBufferMatcher);
            child.matches = (buffer, startOffset) => matchesNode(buffer, startOffset, child.node);
        }
    } else {
        child.pattern = buildKeyValuePattern(keyPrefix, value);
    }
    return child;
}

/**
 * @param {Buffer} keyPrefix Serialized key prefix (`"key":`).
 * @param {any} value Scalar value to append.
 * @returns {Buffer} Full serialized `"key":value` pattern.
 */
function buildKeyValuePattern(keyPrefix, value) {
    return Buffer.concat([keyPrefix, Buffer.from(JSON.stringify(value), 'utf8')]);
}

/**
 * Cheap pass: confirm each child's primary pattern exists somewhere and cache that position as a
 * hint for the depth-aware pass that follows.
 *
 * @param {Buffer} buffer Compact JSON document buffer.
 * @param {number} startOffset Start offset within `buffer`.
 * @param {{children: Array<object>}} node Compiled matcher node for this level.
 * @returns {boolean} True when every child pattern exists somewhere from `startOffset`.
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
 * @param {Buffer} buffer Compact JSON document buffer.
 * @param {number} startOffset Start offset within `buffer`.
 * @param {{children: Array<object>}} node Compiled matcher node for this level.
 * @returns {boolean} True when all children match at the requested JSON level.
 */
function matchesNode(buffer, startOffset, node) {
    for (const child of node.children) {
        const matchPosition = indexOfSameLevel(buffer, child.pattern, startOffset, child.lastMatch, child.isKeyPattern);
        if (matchPosition === -1) {
            return false;
        }

        const valueStart = matchPosition + child.pattern.length;
        if (child.matches && !child.matches(buffer, valueStart)) {
            return false;
        }
    }

    return true;
}

/**
 * Parse the scalar at a matched key position only when operator objects require a real JavaScript
 * comparison instead of a pure byte-pattern check.
 *
 * @param {Buffer} buffer Compact JSON document buffer.
 * @param {number} startOffset Offset of the scalar value to parse.
 * @param {Array<function(any): boolean>} operatorChecks Compiled operator checks.
 * @returns {boolean} True when the parsed scalar satisfies all operators.
 */
function matchesOperatorInBuffer(buffer, startOffset, operatorChecks) {
     const valueStart = startOffset;
     const valueEnd = findJsonValueEnd(buffer, valueStart);
     /* c8 ignore next 2 */
     if (valueEnd === -1 || valueEnd <= valueStart) {
         return false;
     }

    const parsedValue = parseJsonValue(buffer, valueStart, valueEnd);

    return matchesCompiledOperators(parsedValue, operatorChecks);
}

/**
 * Map pre-computed ordering information to operator semantics.
 * ordering: -1 (actual < expected), 0 (equal), 1 (actual > expected)
 *
 * @param {string} operator
 * @param {-1|0|1} ordering
 * @returns {boolean}
 */
function matchesOrdering(operator, ordering) {
     switch (operator) {
         case '$gt':
             return ordering === 1;
         case '$gte':
             return ordering >= 0;
         case '$lt':
             return ordering === -1;
         case '$lte':
             return ordering <= 0;
         case '$eq':
             return ordering === 0;
         case '$ne':
             return ordering !== 0;
         /* c8 ignore next 1 */
         default:
             return false;
     }
 }

/**
 * Build a specialized buffer-based operator comparator by pre-compiling operator-specific
 * byte shortcuts at matcher build time. This avoids runtime dispatch and enables aggressive
 * short-circuit evaluation for sign mismatches and digit-count differences.
 *
 * Assumes no scientific notation in the JSON buffer.
 *
 * @param {object} operatorObj Operator object (e.g., { $gt: 100 }).
 * @returns {function(Buffer, number): boolean} Predicate that reads the buffer at given offset.
 */
function buildOperatorBufferMatcher(operatorObj) {
    const entries = Object.entries(operatorObj);
    const operatorChecks = getCompiledOperatorChecks(operatorObj);

    if (entries.length !== 1) {
        // Multi-operator case: fall back to generic path.
        return (buffer, startOffset) => matchesOperatorInBuffer(buffer, startOffset, operatorChecks);
    }

    const [operator, expectedValue] = entries[0];

    // Single comparison operator on a number: generate a single-pass comparator without parsing.
    if (typeof expectedValue === 'number' && (operator === '$gt' || operator === '$gte' || operator === '$lt' || operator === '$lte')) {
        const expectedStr = JSON.stringify(expectedValue);
        const expectedIsNegative = expectedStr[0] === '-';
        const intStart = expectedIsNegative ? 1 : 0;
        const [expectedIntegerPart, expectedFractionPart = ''] = expectedStr.substring(intStart).split('.');
        const expectedNumeric = {
            isNegative: expectedIsNegative,
            integerPart: expectedIntegerPart,
            fractionPart: expectedFractionPart
        };

        return (buffer, startOffset) => {
             const ordering = compareNumeric(buffer, startOffset, expectedNumeric);
             /* c8 ignore next 2 */
             if (ordering === null) {
                 return false;
             }
             return matchesOrdering(operator, ordering);
         };
    }

    // Non-numeric expected value: use generic operator checks.
    return (buffer, startOffset) => matchesOperatorInBuffer(buffer, startOffset, operatorChecks);
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
