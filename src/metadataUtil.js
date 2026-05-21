import crypto from 'crypto';
import { assertEqual } from './util.js';

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
        if (Array.isArray(matcher[prop])) {
            if (!matcher[prop].includes(document[prop])) {
                return false;
            }
        } else if (typeof matcher[prop] === 'object') {
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
 * Builds a raw-buffer matcher. Flat scalar matchers use a top-level fast path,
 * nested/array matchers use scoped matching.
 *
 * @param {object} matcher Object matcher.
 * @returns {function(Buffer): boolean}
 */
function buildRawBufferMatcher(matcher = {}) {
    if (!matcher || typeof matcher !== 'object' || Array.isArray(matcher)) {
        throw new TypeError('Matcher must be an object.');
    }

    if (isFlatScalarMatcher(matcher)) {
        return buildFlatRawBufferMatcher(matcher);
    }

    return buildScopedRawBufferMatcher(matcher);
}

function isFlatScalarMatcher(matcher) {
    return Object.values(matcher).every(value => !Array.isArray(value) && (!value || typeof value !== 'object'));
}

function buildFlatRawBufferMatcher(matcher) {
    const patterns = Object.entries(matcher).map(([key, value]) => Buffer.from(`${JSON.stringify(key)}:${JSON.stringify(value)}`, 'utf8'));

    return function matchesRawBuffer(buffer) {
        if (!Buffer.isBuffer(buffer)) {
            return false;
        }
        for (const pattern of patterns) {
            if (buffer.indexOf(pattern) === -1) {
                return false;
            }
        }
        return patterns.every(pattern => hasSameLevelMatch(buffer, pattern, 1));
    };
}

/**
 * Builds a scoped raw matcher that resolves nested matcher steps per object scope.
 * It targets compact JSON.stringify payloads and avoids full deserialization.
 *
 * @param {object} matcher Object matcher supporting nested objects and scalar arrays.
 * @returns {function(Buffer): boolean}
 */
function buildScopedRawBufferMatcher(matcher = {}) {
    if (!matcher || typeof matcher !== 'object' || Array.isArray(matcher)) {
        throw new TypeError('Matcher must be an object.');
    }

    const root = buildScopedMatcherNode(matcher);
    const presenceGroups = collectScopedPresenceGroups(root);
    if (root.requiredLeaves === 0) {
        return () => true;
    }

    return function matchesRawBufferScoped(buffer) {
        if (!Buffer.isBuffer(buffer)) {
            return false;
        }
        if (buffer[0] !== 0x7b) { // '{'
            return false;
        }

        for (const group of presenceGroups) {
            let found = false;
            for (const token of group) {
                if (buffer.indexOf(token) !== -1) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return false;
            }
        }

        try {
            return matchScopedNode(buffer, 0, root) === root.requiredLeaves;
        } catch {
            return false;
        }
    };
}

function buildScopedMatcherNode(matcher) {
    const node = { children: new Map(), valuePatterns: null, requiredLeaves: 0 };

    for (const [key, value] of Object.entries(matcher)) {
        const childNode = { children: new Map(), valuePatterns: null, keyValuePatterns: null, requiredLeaves: 0, keyPrefix: Buffer.from(`${JSON.stringify(key)}:`, 'utf8') };

        if (Array.isArray(value)) {
            if (value.some(item => item && typeof item === 'object')) {
                throw new TypeError('Array matcher values must be scalars.');
            }
            childNode.valuePatterns = value.map(item => Buffer.from(JSON.stringify(item), 'utf8'));
            childNode.keyValuePatterns = childNode.valuePatterns.map(pattern => Buffer.concat([childNode.keyPrefix, pattern]));
            childNode.requiredLeaves = 1;
        } else if (value && typeof value === 'object') {
            const nestedNode = buildScopedMatcherNode(value);
            childNode.children = nestedNode.children;
            childNode.valuePatterns = nestedNode.valuePatterns;
            childNode.keyValuePatterns = nestedNode.keyValuePatterns;
            childNode.requiredLeaves = nestedNode.requiredLeaves;
        } else {
            childNode.valuePatterns = [Buffer.from(JSON.stringify(value), 'utf8')];
            childNode.keyValuePatterns = childNode.valuePatterns.map(pattern => Buffer.concat([childNode.keyPrefix, pattern]));
            childNode.requiredLeaves = 1;
        }

        node.children.set(key, childNode);
        node.requiredLeaves += childNode.requiredLeaves;
    }

    return node;
}

function matchScopedNode(buffer, objectStart, node) {
    let i = objectStart + 1;
    let matchedLeaves = 0;
    const matchedByChild = new Map();

    for (const childNode of node.children.values()) {
        if (!childNode.keyValuePatterns) {
            continue;
        }
        const hasMatch = childNode.keyValuePatterns.some(pattern => hasSameLevelMatch(buffer, pattern, objectStart + 1));
        if (hasMatch) {
            matchedByChild.set(childNode, 1);
            matchedLeaves += 1;
        }
    }

    if (matchedLeaves === node.requiredLeaves) {
        return matchedLeaves;
    }

    if (buffer[i] === 0x7d) { // '}'
        return 0;
    }

    while (i < buffer.length) {
        if (buffer[i] !== 0x22) { // '"'
            throw new Error('Invalid JSON object: expected key.');
        }

        const keyEnd = scanJsonString(buffer, i);
        const key = parseJsonKey(buffer, i, keyEnd);
        i = keyEnd;

        if (buffer[i] !== 0x3a) { // ':'
            throw new Error('Invalid JSON object: expected colon.');
        }
        i++;

        const childNode = node.children.get(key);
        if (childNode && childNode.children.size > 0) {
            const childMatchedLeaves = matchScopedChild(buffer, i, childNode);
            const previous = matchedByChild.get(childNode) || 0;
            if (childMatchedLeaves > previous) {
                matchedByChild.set(childNode, childMatchedLeaves);
                matchedLeaves += childMatchedLeaves - previous;
                if (matchedLeaves === node.requiredLeaves) {
                    return matchedLeaves;
                }
            }
        }

        i = skipJsonValue(buffer, i);

        if (buffer[i] === 0x2c) { // ','
            i++;
            continue;
        }
        if (buffer[i] === 0x7d) { // '}'
            return matchedLeaves;
        }

        throw new Error('Invalid JSON object: expected comma or end.');
    }

    throw new Error('Invalid JSON object: unterminated object.');
}

function matchScopedChild(buffer, valueOffset, childNode) {
    let matchedLeaves = 0;

    if (childNode.children.size > 0 && buffer[valueOffset] === 0x7b) { // '{'
        matchedLeaves += matchScopedNode(buffer, valueOffset, childNode);
    }

    return matchedLeaves;
}

function collectScopedPresenceGroups(node, groups = []) {
    for (const childNode of node.children.values()) {
        if (childNode.valuePatterns) {
            groups.push(childNode.valuePatterns.map(pattern => Buffer.concat([childNode.keyPrefix, pattern])));
        }
        if (childNode.children.size > 0) {
            collectScopedPresenceGroups(childNode, groups);
        }
    }
    return groups;
}

function parseJsonKey(buffer, start, end) {
    let hasEscape = false;
    for (let i = start + 1; i < end - 1; i++) {
        if (buffer[i] === 0x5c) { // '\\'
            hasEscape = true;
            break;
        }
    }

    if (!hasEscape) {
        return buffer.toString('utf8', start + 1, end - 1);
    }

    return JSON.parse(buffer.toString('utf8', start, end));
}

function scanJsonString(buffer, offset) {
    let i = offset + 1;
    while (i < buffer.length) {
        if (buffer[i] === 0x5c) { // '\\'
            i += 2;
            continue;
        }
        if (buffer[i] === 0x22) { // '"'
            return i + 1;
        }
        i++;
    }
    throw new Error('Invalid JSON string: unterminated string.');
}

function skipJsonValue(buffer, offset) {
    const ch = buffer[offset];
    if (ch === 0x7b || ch === 0x5b) { // '{' or '['
        return scanCompositeJsonValue(buffer, offset);
    }
    if (ch === 0x22) { // '"'
        return scanJsonString(buffer, offset);
    }
    return scanJsonScalar(buffer, offset);
}

function scanCompositeJsonValue(buffer, offset) {
    let depth = 0;
    let inString = false;
    let i = offset;

    while (i < buffer.length) {
        const ch = buffer[i];
        if (inString) {
            if (ch === 0x5c) { // '\\'
                i += 2;
                continue;
            }
            if (ch === 0x22) { // '"'
                inString = false;
            }
            i++;
            continue;
        }

        if (ch === 0x22) { // '"'
            inString = true;
            i++;
            continue;
        }

        if (ch === 0x7b || ch === 0x5b) { // '{' or '['
            depth++;
            i++;
            continue;
        }

        if (ch === 0x7d || ch === 0x5d) { // '}' or ']'
            depth--;
            i++;
            if (depth === 0) {
                return i;
            }
            continue;
        }

        i++;
    }

    throw new Error('Invalid JSON value: unterminated composite value.');
}

function scanJsonScalar(buffer, offset) {
    let i = offset;
    while (i < buffer.length) {
        const ch = buffer[i];
        if (ch === 0x2c || ch === 0x7d || ch === 0x5d) { // ',' or '}' or ']'
            return i;
        }
        i++;
    }
    return i;
}

function hasSameLevelMatch(buffer, pattern, startOffset = 0) {
    let depth = 0;
    let inString = false;
    let i = startOffset;

    while (i < buffer.length) {
        if (inString) {
            if (buffer[i] === 0x5c) { // '\\'
                i += 2;
                continue;
            }
            if (buffer[i] === 0x22) { // '"'
                inString = false;
            }
            i++;
            continue;
        }

        const ch = buffer[i];
        if (ch === 0x7b || ch === 0x5b) { // '{' or '['
            depth++;
            i++;
            continue;
        }
        if (ch === 0x7d || ch === 0x5d) { // '}' or ']'
            depth--;

            if (depth < 0) {
                return false;
            }

            i++;
            continue;
        }

        if (ch === 0x22 && depth === 0 && startsWithAt(buffer, i, pattern)) { // '"'
            const end = i + pattern.length;
            if (buffer[end] === 0x2c || buffer[end] === 0x7d || buffer[end] === 0x5d) { // ',' or '}' or ']'
                return true;
            }
        }

        if (ch === 0x22) { // '"'
            inString = true;
        }
        i++;
    }

    return false;
}

function startsWithAt(buffer, offset, pattern) {
    if (offset < 0 || offset + pattern.length > buffer.length) {
        return false;
    }
    return buffer.compare(pattern, 0, pattern.length, offset, offset + pattern.length) === 0;
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
