import { getPropAtPath } from './util.js';

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
 * When the discriminant property list is empty, `getCandidates()` returns `null`
 * to signal that the caller should fall back to a full O(N) scan.
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
        if (typeof matcher === 'function') {
            this.functionMatchers.add(indexName);
            return;
        }
        if (matcher && typeof matcher === 'object') {
            const discriminant = this.findDiscriminant(matcher);
            if (discriminant) {
                let propMap = this.table.get(discriminant.propPath);
                if (!propMap) {
                    propMap = new Map();
                    this.table.set(discriminant.propPath, propMap);
                }
                let indexSet = propMap.get(discriminant.value);
                if (!indexSet) {
                    indexSet = new Set();
                    propMap.set(discriminant.value, indexSet);
                }
                indexSet.add(indexName);
                return;
            }
        }
        // null/undefined matcher or object with no usable discriminant property.
        this.unclassifiedMatchers.add(indexName);
    }

    /**
     * Remove an index name from the lookup structures.
     *
     * @param {string} indexName
     * @param {Matcher} matcher
     */
    remove(indexName, matcher) {
        if (typeof matcher === 'function') {
            this.functionMatchers.delete(indexName);
            return;
        }
        if (matcher && typeof matcher === 'object') {
            const discriminant = this.findDiscriminant(matcher);
            if (discriminant) {
                const propMap = this.table.get(discriminant.propPath);
                if (propMap) {
                    const indexSet = propMap.get(discriminant.value);
                    if (indexSet) {
                        indexSet.delete(indexName);
                        if (indexSet.size === 0) propMap.delete(discriminant.value);
                    }
                    if (propMap.size === 0) this.table.delete(discriminant.propPath);
                }
                return;
            }
        }
        this.unclassifiedMatchers.delete(indexName);
    }

    /**
     * Return the set of candidate index names for the given document.
     *
     * When `this.properties` is non-empty, performs an O(1) lookup for each
     * configured property path and unions the resulting candidate sets, then
     * adds all unclassified and function-matcher index names.
     *
     * Returns `null` when `this.properties` is empty, signalling that the caller
     * should fall back to iterating all registered secondary indexes.
     *
     * @param {object} document
     * @returns {Set<string>|null}
     */
    getCandidates(document) {
        if (this.properties.length === 0) {
            return null;
        }

        const candidates = new Set();

        for (const propPath of this.properties) {
            const docValue = getPropAtPath(document, propPath);
            if (docValue !== undefined && docValue !== null && typeof docValue !== 'object') {
                const propMap = this.table.get(propPath);
                if (propMap) {
                    const indexSet = propMap.get(String(docValue));
                    if (indexSet) {
                        for (const name of indexSet) candidates.add(name);
                    }
                }
            }
        }

        for (const name of this.unclassifiedMatchers) candidates.add(name);
        for (const name of this.functionMatchers) candidates.add(name);

        return candidates;
    }

    /**
     * Find the first usable discriminant for an object matcher.
     * Returns `{ propPath, value }` for the first entry in `this.properties` that
     * resolves to a non-null, non-object scalar inside `matcher`, or `null` if none.
     *
     * @param {object} matcher
     * @returns {{ propPath: string, value: string }|null}
     */
    findDiscriminant(matcher) {
        for (const propPath of this.properties) {
            const value = getPropAtPath(matcher, propPath);
            if (value !== undefined && value !== null && typeof value !== 'object') {
                return { propPath, value: String(value) };
            }
        }
        return null;
    }

}

export default IndexMatcher;
