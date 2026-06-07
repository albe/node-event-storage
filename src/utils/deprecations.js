const emittedDeprecationWarnings = new Set();

/**
 * Emit a deprecation warning only once per warning code.
 *
 * @param {string} code Unique warning code.
 * @param {string} message Warning message.
 * @returns {void}
 */
function emitDeprecationWarningOnce(code, message) {
    if (emittedDeprecationWarnings.has(code)) {
        return;
    }
    emittedDeprecationWarnings.add(code);
    process.emitWarning(message, 'DeprecationWarning', code);
}

export { emitDeprecationWarningOnce };

