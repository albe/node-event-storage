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

function preCheck(buffer, startOffset, node) {
    for (const child of node.children) {
        let i = 0;
        if (child.valuePatterns && !child.valuePatterns.some(pattern => (child.valueMatches[i++] = buffer.indexOf(pattern, startOffset)) !== -1)) {
            return false;
        }
        if (child.objectPattern) {
            if ((child.objMatch = buffer.indexOf(child.objectPattern, startOffset)) === -1) {
                return false;
            }
            if (!preCheck(buffer, child.objMatch, child.node)) {
                return false;
            }
        }
    }
    return true;
}

function buildMatcherTree(matcher) {
    const node = { children: [] };

    for (const [key, value] of Object.entries(matcher)) {
        const keyPrefix = Buffer.from(`${JSON.stringify(key)}:`, 'utf8');
        const child = { objectPattern: null, valuePatterns: null, node: null, objMatch: null, valueMatches: [] };

        if (Array.isArray(value)) {
            if (value.some(item => item && typeof item === 'object')) {
                throw new TypeError('Array matcher values must be scalars.');
            }
            child.valuePatterns = value.map(item => Buffer.concat([keyPrefix, Buffer.from(JSON.stringify(item), 'utf8')]));
        } else if (value && typeof value === 'object') {
            child.objectPattern = Buffer.concat([keyPrefix, Buffer.from('{', 'utf8')]);
            child.node = buildMatcherTree(value);
        } else {
            child.valuePatterns = [Buffer.concat([keyPrefix, Buffer.from(JSON.stringify(value), 'utf8')])];
        }

        node.children.push(child);
    }

    return node;
}

function matchesNode(buffer, startOffset, node) {
    for (const child of node.children) {
        if (child.valuePatterns) {
            let i = 0;
            if (!child.valuePatterns.some(pattern => indexOfSameLevel(buffer, pattern, startOffset, child.valueMatches[i++]) !== -1)) {
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
        } else if (ch === 0x7d || ch === 0x5d) { // '}' or ']'
            depth--;

            if (depth < 0) {
                return -1;
            }

            i++;
            continue;
        } else if (ch === 0x22) { // '"'
            inString = true;
        }

        if (i >= matchPosition) {
            if (i === matchPosition && ch === 0x22 && depth === 0) { // '"'
                const end = i + pattern.length;
                if (pattern[pattern.length - 1] === 0x7b) { // '{'
                    return i;
                }
                if (buffer[end] === 0x2c || buffer[end] === 0x7d || buffer[end] === 0x5d) { // ',' or '}' or ']'
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
