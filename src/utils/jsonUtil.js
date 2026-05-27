const BYTE_QUOTE = 0x22;
const BYTE_ESCAPE = 0x5c;
const BYTE_OPEN_OBJECT = 0x7b;
const BYTE_CLOSE_OBJECT = 0x7d;
const BYTE_OPEN_ARRAY = 0x5b;
const BYTE_CLOSE_ARRAY = 0x5d;
const BYTE_COMMA = 0x2c;

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

export { BYTE_OPEN_OBJECT, indexOfSameLevel };
