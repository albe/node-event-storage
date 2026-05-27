import crypto from 'crypto';
import { assertEqual } from './util.js';

const BYTE_QUOTE = 0x22;
const BYTE_ESCAPE = 0x5c;
const BYTE_OPEN_OBJECT = 0x7b;
const BYTE_CLOSE_OBJECT = 0x7d;
const BYTE_OPEN_ARRAY = 0x5b;
const BYTE_CLOSE_ARRAY = 0x5d;
const BYTE_COMMA = 0x2c;

function isPlainObject(value) {
    return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function propertyMatchesValue(documentValue, matcherValue) {
    if (Array.isArray(matcherValue)) {
        return matcherValue.includes(documentValue);
    }
    if (isPlainObject(matcherValue)) {
        return matches(documentValue, matcherValue);
    }
    return typeof matcherValue === 'undefined' || documentValue === matcherValue;
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
 * Builds a raw-buffer matcher.
 * It expects the Buffer to contain compact stringified JSON
 * and supports matcher objects with sub properties and multi-value matches (OR/any of).
 *
 * @param {object} matcher Object matcher.
 * @returns {function(Buffer): boolean}
 */
function buildRawBufferMatcher(matcher = {}) {
    if (!matcher || typeof matcher !== 'object' || Array.isArray(matcher)) {
        throw new TypeError('Matcher must be an object.');
    }

    const root = buildMatcherTree(matcher);
    if (root.children.length === 0) {
        return () => true;
    }

    return function matchesRawBuffer(buffer) {
        if (buffer[0] !== 0x7b) { // '{'
            return false;
        }
        if (!preCheck(buffer, 1, root)) {
            return false;
        }
        return matchesNode(buffer, 1, root);
    };
}

/**
 * Optimization pass: check that every required byte pattern is present anywhere in the buffer
 * before spending the more expensive per-depth scan in `matchesNode`.
 */
function preCheck(buffer, startOffset, node) {
    for (const child of node.children) {
        if (child.valuePatterns && !populatePatternMatches(buffer, startOffset, child.valuePatterns, child.valueMatches)) {
            return false;
        }
        if (child.objectPattern) {
            const objectMatch = buffer.indexOf(child.objectPattern, startOffset);
            if (objectMatch === -1) {
                return false;
            }
            child.objMatch = objectMatch;
            if (!preCheck(buffer, objectMatch, child.node)) {
                return false;
            }
        }
    }
    return true;
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

function buildMatcherTreeChild(key, value) {
    const keyPrefix = Buffer.from(`${JSON.stringify(key)}:`, 'utf8');
    const child = { objectPattern: null, valuePatterns: null, node: null, objMatch: null, valueMatches: [] };
    if (Array.isArray(value)) {
        if (value.some(item => item && typeof item === 'object')) {
            throw new TypeError('Array matcher values must be scalars.');
        }
        child.valuePatterns = value.map(item => buildValuePattern(keyPrefix, item));
        return child;
    }
    if (isPlainObject(value)) {
        child.objectPattern = Buffer.concat([keyPrefix, Buffer.from('{', 'utf8')]);
        child.node = buildMatcherTree(value);
        return child;
    }
    child.valuePatterns = [buildValuePattern(keyPrefix, value)];
    return child;
}

function buildValuePattern(keyPrefix, value) {
    return Buffer.concat([keyPrefix, Buffer.from(JSON.stringify(value), 'utf8')]);
}

/**
 * Verify that each required byte pattern in the tree is present at the correct JSON nesting
 * depth so values inside nested objects don't satisfy a top-level match requirement.
 */
function matchesNode(buffer, startOffset, node) {
    for (const child of node.children) {
        if (child.valuePatterns) {
            if (!findSameLevelPatternOffset(buffer, startOffset, child.valuePatterns, child.valueMatches)) {
                return false;
            }
        }

        if (child.node) {
            const objectIndex = indexOfSameLevel(buffer, child.objectPattern, startOffset, child.objMatch);
            if (objectIndex === -1) {
                return false;
            }
            if (!matchesNode(buffer, objectIndex + child.objectPattern.length, child.node)) {
                return false;
            }
        }
    }

    return true;
}

function populatePatternMatches(buffer, startOffset, patterns, matchesOut) {
    for (let i = 0; i < patterns.length; i++) {
        const match = buffer.indexOf(patterns[i], startOffset);
        matchesOut[i] = match;
        if (match !== -1) {
            return true;
        }
    }
    return false;
}

function findSameLevelPatternOffset(buffer, startOffset, patterns, preMatches) {
    for (let i = 0; i < patterns.length; i++) {
        if (indexOfSameLevel(buffer, patterns[i], startOffset, preMatches[i]) !== -1) {
            return true;
        }
    }
    return false;
}

/**
 * Find the position of `pattern` within `buffer` at depth 0 (the top-level object), starting
 * from `startOffset`.  It scans character-by-character tracking JSON nesting depth and string
 * quoting.  If `matchPosition` arrives at depth > 0 it means the pattern is inside a nested
 * object/array, so the scan continues searching for the next candidate at depth 0.  Returns -1
 * when no such position exists before the end of the buffer or when a closing brace reduces depth
 * below zero (the top-level object has ended).
 */
function indexOfSameLevel(buffer, pattern, startOffset = 0, matchPosition) {
    /* c8 ignore start */
    // Defensive fallback: public call path precomputes an initial candidate in preCheck.
    if (matchPosition === undefined) {
        matchPosition = buffer.indexOf(pattern, startOffset);
    }
    if (matchPosition === -1) {
        return -1;
    }
    /* c8 ignore stop */

    let depth = 0;
    let inString = false;
    let i = startOffset;

    while (i < buffer.length) {
        if (inString) {
            if (buffer[i] === BYTE_ESCAPE) { // '\\'
                i += 2;
                continue;
            }
            if (buffer[i] === BYTE_QUOTE) { // '"'
                inString = false;
            }
            i++;
            continue;
        }

        const ch = buffer[i];
        if (ch === BYTE_OPEN_OBJECT || ch === BYTE_OPEN_ARRAY) { // '{' or '['
            depth++;
            i++;
            continue;
        } else if (ch === BYTE_CLOSE_OBJECT || ch === BYTE_CLOSE_ARRAY) { // '}' or ']'
            depth--;

            if (depth < 0) {
                return -1;
            }

            i++;
            continue;
        } else if (ch === BYTE_QUOTE) { // '"'
            inString = true;
        }

        if (i >= matchPosition) {
            if (i === matchPosition && ch === BYTE_QUOTE && depth === 0) { // '"'
                const end = i + pattern.length;
                if (pattern[pattern.length - 1] === BYTE_OPEN_OBJECT) { // '{'
                    return i;
                }
                if (buffer[end] === BYTE_COMMA || buffer[end] === BYTE_CLOSE_OBJECT || buffer[end] === BYTE_CLOSE_ARRAY) { // ',' or '}' or ']'
                    return i;
                }
            }

            matchPosition = buffer.indexOf(pattern, matchPosition + 1);
            if (matchPosition < 0) {
                return -1;
            }
        }

        i++;
    }

    /* c8 ignore next */
    return -1;
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
