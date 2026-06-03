const BYTE_QUOTE = 0x22;
const BYTE_ESCAPE = 0x5c;
const BYTE_OPEN_OBJECT = 0x7b;
const BYTE_CLOSE_OBJECT = 0x7d;
const BYTE_OPEN_ARRAY = 0x5b;
const BYTE_CLOSE_ARRAY = 0x5d;
const BYTE_COMMA = 0x2c;

/**
 * Advance past a JSON string whose opening `"` is at `i`.
 * Returns the position after the closing `"`, or -1 if the string is unterminated.
 */
function skipString(buffer, i) {
    let j = i + 1;
    while (j < buffer.length) {
        if (buffer[j] === BYTE_ESCAPE) {
            j += 2;
            continue;
        }
        if (buffer[j] === BYTE_QUOTE) {
            return j + 1;
        }
        j++;
    }
    /* c8 ignore next */
    return -1;
}

/**
 * Check if a character byte is a valid JSON value delimiter (comma, closing brace, or closing bracket).
 * @param {number} char
 * @returns {boolean}
 */
function isDelimiter(char) {
    return (char === BYTE_COMMA || char === BYTE_CLOSE_OBJECT || char === BYTE_CLOSE_ARRAY);
}

/**
 * Find the position of `pattern` within `buffer` at depth 0 (the top-level object), starting
 * from `startOffset`. Tracks JSON nesting depth and skips over string contents entirely.
 * If `matchPosition` arrives at depth > 0 it means the pattern is inside a nested
 * object/array, so the scan continues searching for the next candidate at depth 0.
 *
 * For value patterns (`"key":value`) it validates the trailing delimiter to avoid prefix matches.
 * For key patterns (`"key":`) pass `isKeyPattern=true` to skip that trailing delimiter check.
 * Returns -1 when no such position exists before the end of the buffer or when a closing brace
 * reduces depth below zero (the top-level object has ended).
 */
function indexOfSameLevel(buffer, pattern, startOffset = 0, matchPosition, isKeyPattern = false) {
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
    let i = startOffset;

    while (i < buffer.length) {
        const ch = buffer[i];

        if (ch === BYTE_OPEN_OBJECT || ch === BYTE_OPEN_ARRAY) {
            depth++;
            i++;
            continue;
        }
        if (ch === BYTE_CLOSE_OBJECT || ch === BYTE_CLOSE_ARRAY) {
            depth--;
            if (depth < 0) {
                return -1;
            }
            i++;
            continue;
        }
        if (ch === BYTE_QUOTE) {
            if (i === matchPosition && depth === 0) {
                if (isKeyPattern) {
                    return i;
                }
                const end = i + pattern.length;
                if (pattern[pattern.length - 1] === BYTE_OPEN_OBJECT) {
                    return i;
                }
                if (isDelimiter(buffer[end])) {
                    return i;
                }
            }
            i = skipString(buffer, i);
            /* c8 ignore next */
            if (i === -1) {
                return -1;
            }
            if (matchPosition < i) {
                matchPosition = buffer.indexOf(pattern, i);
                if (matchPosition === -1) {
                    return -1;
                }
            }
            continue;
        }

        i++;
    }

    /* c8 ignore next */
    return -1;
}

/**
 * Extract the end position (exclusive) of a JSON scalar value starting at `offset`.
 * `offset` should point to the first character of the value ('"' for strings, digit/-/true/false/null for others).
 * Returns the position after the value ends (past the closing quote for strings, past the last digit/char for others).
 * Returns -1 if the buffer is malformed.
 */
function findJsonValueEnd(buffer, offset) {
    /* c8 ignore next */
    if (offset >= buffer.length) {
        return -1;
    }

    if (buffer[offset] === BYTE_QUOTE) {
        return skipString(buffer, offset);
    }

    // Number, boolean, or null: scan until delimiter (,}])
    let i = offset;
    while (i < buffer.length && !isDelimiter(buffer[i])) i++;
    return i;
}

/**
 * Convert a JSON scalar value buffer to a comparable JavaScript value for range operators.
 * Supports strings, numbers, booleans, and null.
 */
function parseJsonValue(buffer, startOffset, endOffset) {
    const valueStr = buffer.toString('utf8', startOffset, endOffset).trim();
    if (!valueStr) {
        return undefined;
    }
    try {
        return JSON.parse(valueStr);
    } catch {
        return undefined;
    }
}

export { BYTE_OPEN_OBJECT, BYTE_CLOSE_OBJECT, indexOfSameLevel, findJsonValueEnd, parseJsonValue };
