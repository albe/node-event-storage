import { getPropertyAtPath } from './utils/util.js';
import { matches } from './utils/metadataUtil.js';

/**
 * @param {any} value Candidate matcher value at a discriminant path.
 * @returns {boolean} True when `value` is `{ $has: scalar }` (lone $has with a non-object value).
 */
function isLoneHasScalar(value) {
    if (Array.isArray(value)) return false;
    const keys = Object.keys(value);
    if (keys.length !== 1 || keys[0] !== '$has') return false;
    const has = value.$has;
    return has !== null && has !== undefined && typeof has !== 'object';
}

/**
 * @param {any} value Candidate matcher value at a discriminant path.
 * @returns {boolean} True when `value` is `{ $hasAny: [scalar, ...] }` (lone $hasAny with a non-empty scalar array).
 */
function isLoneHasAnyArray(value) {
    if (Array.isArray(value)) return false;
    const keys = Object.keys(value);
    if (keys.length !== 1 || keys[0] !== '$hasAny') return false;
    const hasAny = value.$hasAny;
    return Array.isArray(hasAny) && hasAny.length > 0 &&
        hasAny.every(v => v !== null && v !== undefined && typeof v !== 'object');
}

/**
 * @typedef {object|function(object):boolean} Matcher
 */

/**
 * Classifies secondary-index matchers into a fast lookup table keyed on a
 * configurable ordered list of "discriminant" property paths.  This enables O(1)
 * candidate resolution on write instead of evaluating every registered matcher.
 *
 * Object matchers that contain at least one of the discriminant property paths
 * (in priority order) are stored in a nested Map keyed first by property path and
 * then by the scalar value found at that path in the matcher.  Function matchers
 * and object matchers whose discriminant properties all resolve to undefined/object
 * are kept in separate fallback sets and are always evaluated in full.
 *
 * When the discriminant property list is empty, `forEachMatch()` falls back to a
 * full O(N) scan over all registered indexes.
 */
class IndexMatcher {

    /**
     * @param {string[]} [properties] Ordered list of document property paths (dot-notation)
     *   used as discriminant keys.  The first path that resolves to a non-null scalar inside
     *   a given object matcher is used as the key; remaining paths are ignored for that matcher.
     *   Pass an empty array (the default) to disable the fast path entirely.
     */
    constructor(properties = []) {
        this.properties = properties;
        /** Map<indexName, Matcher> — stores every registered matcher for full match verification. */
        this.matchers = new Map();
        /**
         * Nested lookup table: Map<propPath, Map<discriminantValue, Set<indexName>>>.
         * Populated only for object matchers that contain at least one discriminant property.
         */
        this.table = new Map();
        /** Set of index names whose matchers are functions (always evaluated in full). */
        this.functionMatchers = new Set();
        /**
         * Set of index names whose object matchers contain none of the configured
         * discriminant properties (evaluated in full against every incoming document).
         */
        this.unclassifiedMatchers = new Set();
    }

    /**
     * Register an index name and its matcher in the lookup structures.
     *
     * @param {string} indexName
     * @param {Matcher} matcher
     */
    add(indexName, matcher) {
        this.matchers.set(indexName, matcher);
        if (typeof matcher === 'function') {
            this.functionMatchers.add(indexName);
            return;
        }
        if (!matcher || typeof matcher !== 'object') {
            this.unclassifiedMatchers.add(indexName);
            return;
        }

        const discriminant = this.findDiscriminant(matcher);
        if (!discriminant) {
            this.unclassifiedMatchers.add(indexName);
            return;
        }

        let propMap = this.table.get(discriminant.propPath);
        if (!propMap) {
            propMap = new Map();
            this.table.set(discriminant.propPath, propMap);
        }
        for (const v of discriminant.values) {
            let indexSet = propMap.get(v);
            if (!indexSet) {
                indexSet = new Set();
                propMap.set(v, indexSet);
            }
            indexSet.add(indexName);
        }
    }

    /**
     * Remove an index name from the lookup structures.
     * The matcher is retrieved from the internal registry, so only the index name
     * is required.
     *
     * @param {string} indexName
     */
    remove(indexName) {
        if (!this.matchers.has(indexName)) {
            return;
        }
        const matcher = this.matchers.get(indexName);
        this.matchers.delete(indexName);
        if (typeof matcher === 'function') {
            this.functionMatchers.delete(indexName);
            return;
        }
        if (!matcher || typeof matcher !== 'object') {
            this.unclassifiedMatchers.delete(indexName);
            return;
        }

        const discriminant = this.findDiscriminant(matcher);
        if (!discriminant) {
            this.unclassifiedMatchers.delete(indexName);
            return;
        }

        for (const v of discriminant.values) {
            this.table.get(discriminant.propPath)?.get(v)?.delete(indexName);
        }
    }

    /**
     * Iterate over every registered index whose matcher matches `document`, calling
     * `iterationHandler` with the index name for each match.
     *
     * When `this.properties` is non-empty, an O(1) discriminant lookup narrows the
     * candidate set before the full `matches()` check is applied.  Each candidate's
     * matcher is still verified with `matches()` to handle multi-property matchers
     * where only the first property was used as the discriminant.
     *
     * When `this.properties` is empty the method falls back to a full O(N) scan.
     *
     * @param {object} document
     * @param {function(string): void} iterationHandler Called with the index name for each match.
     */
    forEachMatch(document, iterationHandler) {
        if (this.properties.length === 0) {
            // Fast path disabled: full O(N) scan.
            for (const [indexName, matcher] of this.matchers) {
                if (matches(document, matcher)) {
                    iterationHandler(indexName);
                }
            }
            return;
        }

        for (const propPath of this.properties) {
            const docValue = getPropertyAtPath(document, propPath);
            if (docValue === undefined || docValue === null) {
                continue;
            }

            if (Array.isArray(docValue)) {
                // Multi-value document property: each array element is a potential discriminant.
                // A dedup set prevents calling iterationHandler twice when two elements map to
                // the same index (e.g. duplicate tags, or a single index registered for both).
                const called = new Set();
                for (const item of docValue) {
                    if (item === null || item === undefined || typeof item === 'object') continue;
                    const indexSet = this.table.get(propPath)?.get(String(item));
                    if (!indexSet) continue;
                    for (const indexName of indexSet) {
                        if (!called.has(indexName) && matches(document, this.matchers.get(indexName))) {
                            called.add(indexName);
                            iterationHandler(indexName);
                        }
                    }
                }
                continue;
            }

            if (typeof docValue === 'object') {
                continue;
            }

            const indexSet = this.table.get(propPath)?.get(String(docValue));
            if (!indexSet) {
                continue;
            }

            for (const indexName of indexSet) {
                if (matches(document, this.matchers.get(indexName))) {
                    iterationHandler(indexName);
                }
            }
        }

        for (const indexName of this.unclassifiedMatchers) {
            if (matches(document, this.matchers.get(indexName))) {
                iterationHandler(indexName);
            }
        }

        for (const indexName of this.functionMatchers) {
            if (matches(document, this.matchers.get(indexName))) {
                iterationHandler(indexName);
            }
        }
    }

    /**
     * Find the first usable discriminant for an object matcher.
     * Returns `{ propPath, values }` for the first entry in `this.properties` that
     * resolves to a non-null, non-object scalar (or a non-empty array of scalars) inside
     * `matcher`, or `null` if none.
     *
     * For a scalar discriminant `values` contains exactly one element.
     * For an array-valued discriminant `values` contains all elements of the array.
     *
     * @param {object} matcher
     * @returns {{ propPath: string, values: string[] }|null}
     */
    findDiscriminant(matcher) {
        for (const propPath of this.properties) {
            const value = getPropertyAtPath(matcher, propPath);
            if (value !== undefined && value !== null) {
                if (typeof value !== 'object') {
                    return { propPath, values: [String(value)] };
                }
                if (Array.isArray(value) && value.length > 0 &&
                    value.every(v => v !== null && v !== undefined && typeof v !== 'object')) {
                    return { propPath, values: value.map(String) };
                }
                // Lone $has operator: array-containment matcher (e.g. tag streams). Treat the
                // has-value as the discriminant so array-valued documents route in O(1).
                if (isLoneHasScalar(value)) {
                    return { propPath, values: [String(value.$has)] };
                }
                // Lone $hasAny: register one matcher under each expected value so any of them
                // can trigger O(1) routing when the document array contains that value.
                if (isLoneHasAnyArray(value)) {
                    return { propPath, values: value.$hasAny.map(String) };
                }
            }
        }
        return null;
    }

}

export default IndexMatcher;
