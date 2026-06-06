const BYTE_QUOTE = 0x22;
const BYTE_ESCAPE = 0x5c;
const BYTE_OPEN_OBJECT = 0x7b;
const BYTE_CLOSE_OBJECT = 0x7d;
const BYTE_OPEN_ARRAY = 0x5b;
const BYTE_CLOSE_ARRAY = 0x5d;
const BYTE_COMMA = 0x2c;
const BYTE_SIGN_MINUS = 0x2d;
const BYTE_DECIMAL_SEP = 0x2e;

/**
 * Advance past a JSON string whose opening `"` is at `i`.
 * Returns the position after the closing `"`, or -1 if the string is unterminated.
 *
 * @param {Buffer} buffer Source JSON buffer.
 * @param {number} i Offset of the opening quote.
 * @returns {number} Offset after the closing quote, or -1 if unterminated.
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
 * @param {number} char Byte value to test.
 * @returns {boolean} True when `char` is `,`, `}` or `]`.
 */
function isDelimiter(char) {
    return (char === BYTE_COMMA || char === BYTE_CLOSE_OBJECT || char === BYTE_CLOSE_ARRAY);
}

/**
 * @param {number} char Byte value to test.
 * @returns {boolean} True when `char` opens an object or array.
 */
function isOpeningBracket(char) {
    return char === BYTE_OPEN_OBJECT || char === BYTE_OPEN_ARRAY;
}

/**
 * @param {number} char Byte value to test.
 * @returns {boolean} True when `char` closes an object or array.
 */
function isClosingBracket(char) {
    return char === BYTE_CLOSE_OBJECT || char === BYTE_CLOSE_ARRAY;
}

/**
 * @param {number} char Byte value to test.
 * @returns {boolean} True when `char` is `{`.
 */
function isOpeningObject(char) {
    return char === BYTE_OPEN_OBJECT;
}

/**
 * @param {Buffer} buffer Source JSON buffer.
 * @param {Buffer} pattern Pattern to find.
 * @param {number} startOffset Search start offset.
 * @param {number|undefined} lastMatchPosition Optional cached candidate position.
 * @returns {number} Match position or -1.
 */
function nextIndexOf(buffer, pattern, startOffset, lastMatchPosition) {
    if (lastMatchPosition === undefined || lastMatchPosition < startOffset) {
        return buffer.indexOf(pattern, startOffset);
    }
    return lastMatchPosition;
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
 *
 * @param {Buffer} buffer Source JSON buffer.
 * @param {Buffer} pattern Serialized key/value pattern.
 * @param {number} [startOffset=0] Offset where scanning begins.
 * @param {number|undefined} [matchPosition=undefined] Optional cached candidate position.
 * @param {boolean} [isKeyPattern=false] Skip value-delimiter validation for key-only patterns.
 * @returns {number} Match position at the same JSON depth, or -1.
 */
function indexOfSameLevel(buffer, pattern, startOffset = 0, matchPosition = undefined, isKeyPattern = false) {
    let depth = 0;
    let i = startOffset;

    while (i < buffer.length) {
        matchPosition = nextIndexOf(buffer, pattern, i, matchPosition);
        if (matchPosition === -1) {
            return -1;
        }
        const ch = buffer[i];

        if (isOpeningBracket(ch)) {
            depth++;
            i++;
            continue;
        }
        if (isClosingBracket(ch)) {
            depth--;
            if (depth < 0) {
                return -1;
            }
            i++;
            continue;
        }
        if (ch === BYTE_QUOTE) {
            if (i === matchPosition && depth === 0) {
                if (isKeyPattern || isOpeningObject(pattern[pattern.length - 1])) {
                    return i;
                }
                const end = i + pattern.length;
                if (isDelimiter(buffer[end])) {
                    return i;
                }
            }
            i = skipString(buffer, i);
            /* c8 ignore next 3 */
            if (i === -1) {
                return -1;
            }
            continue;
        }

        i++;
    }

    /* c8 ignore next */
    return -1;
}

/**
 * Find the end of a scalar JSON value so operator matching can parse only the relevant slice.
 *
 * @param {Buffer} buffer Source JSON buffer.
 * @param {number} offset Offset where the scalar starts.
 * @returns {number} End offset (exclusive), or -1 for invalid start.
 */
function findJsonValueEnd(buffer, offset) {
    /* c8 ignore next 3 */
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
 * Parse one scalar JSON slice only after a byte-level match has already narrowed the candidate.
 *
 * @param {Buffer} buffer Source JSON buffer.
 * @param {number} startOffset Start offset (inclusive).
 * @param {number} endOffset End offset (exclusive).
 * @returns {string|number|boolean|null|undefined} Parsed scalar, or `undefined` on parse failure.
 */
function parseJsonValue(buffer, startOffset, endOffset) {
    try {
        const valueStr = buffer.toString('utf8', startOffset, endOffset);
        return JSON.parse(valueStr);
    } catch {
        return undefined;
    }
}

/**
 * @param {number} byte
 * @returns {boolean} True when `byte` is an ASCII digit.
 */
function isAsciiDigit(byte) {
    return byte >= 0x30 && byte <= 0x39;
}

/**
 * Compare a contiguous ASCII digit sequence in `buffer` against expected digits.
 * For JSON.stringify()-normalized numbers this is enough for both integer and fraction parts.
 * Returns only the ordering; when ordering === 0, callers can compute the consumed length as startOffset + expectedDigits.length.
 *
 * @param {Buffer} buffer
 * @param {number} startOffset
 * @param {string} expectedDigits
 * @returns {-1|0|1}
 */
function compareDigits(buffer, startOffset, expectedDigits) {
    let index = startOffset;
    let position = 0;
    let ordering = 0;
    const expectedLength = expectedDigits.length;

    while (index < buffer.length && isAsciiDigit(buffer[index])) {
        if (ordering === 0 && position < expectedLength) {
            const expectedByte = expectedDigits.charCodeAt(position);
            if (buffer[index] !== expectedByte) {
                ordering = buffer[index] < expectedByte ? -1 : 1;
            }
        }
        position++;
        index++;
    }

    if (position !== expectedLength) {
        ordering = position > expectedLength ? 1 : -1;
    }

    return ordering;
}

/**
 * Compare a compact JSON numeric token in `buffer` against a precompiled expected number,
 * using one linear pass over the buffer slice and no `parseJsonValue` call.
 *
 * @param {Buffer} buffer
 * @param {number} startOffset
 * @param {{isNegative: boolean, integerPart: string, fractionPart: string}} expected
 * @returns {-1|0|1|null} Ordering (`actual` vs `expected`) or null for invalid/non-numeric token.
 */
function compareNumeric(buffer, startOffset, expected) {
    let index = startOffset;
    const firstByte = buffer[index];
    if (firstByte !== BYTE_SIGN_MINUS && !isAsciiDigit(firstByte)) {
        return null;
    }

    const isNegative = firstByte === BYTE_SIGN_MINUS;
    if (isNegative !== expected.isNegative) {
        return isNegative ? -1 : 1;
    }

    if (isNegative) {
        index++;
    }

    let result = compareDigits(buffer, index, expected.integerPart);
    if (result === 0) {
        index += expected.integerPart.length;

        const hasFraction = index < buffer.length && buffer[index] === BYTE_DECIMAL_SEP;
        const expectedHasFraction = expected.fractionPart.length > 0;
        if (hasFraction !== expectedHasFraction) {
            result = hasFraction ? 1 : -1;
        } else if (hasFraction) {
            result = compareDigits(buffer, index + 1, expected.fractionPart);
        }
    }
    return isNegative ? -result : result;
}

/**
 * Compare a matched key's scalar value against pre-serialized candidates without reparsing JSON.
 *
 * @param {Buffer} buffer Source JSON buffer.
 * @param {number} valueStart Offset where the candidate scalar begins.
 * @param {Buffer[]} patterns Pre-serialized scalar candidates.
 * @returns {boolean} True when any candidate matches exactly and is delimiter-terminated.
 */
function matchesAnyValuePattern(buffer, valueStart, patterns) {
    for (const pattern of patterns) {
        const valueEnd = valueStart + pattern.length;
        if (valueEnd > buffer.length) {
            continue;
        }
        let matches = true;
        for (let i = 0; i < pattern.length; i++) {
            if (buffer[valueStart + i] !== pattern[i]) {
                matches = false;
                break;
            }
        }
        if (!matches) {
            continue;
        }
        if (isDelimiter(buffer[valueEnd])) {
            return true;
        }
    }
    return false;
}

export { isOpeningObject, indexOfSameLevel, findJsonValueEnd, parseJsonValue, compareNumeric, matchesAnyValuePattern };
