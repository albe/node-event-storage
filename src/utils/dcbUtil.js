/**
 * Determine whether a value is a DcbQuery object (plain object with an `items` property).
 *
 * @param {any} value
 * @returns {boolean}
 */
function isDcbQuery(value) {
    return value !== null && typeof value === 'object' && !Array.isArray(value) && 'items' in value;
}

/**
 * Compile a {@link DcbQuery} into a selector-algebra array suitable for JoinEventStream.
 *
 * Each item in `query.items` produces one sub-selector (OR at the top level).
 * Within an item, tags and types are combined with AND semantics; multiple types form a nested OR.
 *
 * @param {object} query - Object with a non-empty `items` array.
 * @param {function(string): string} resolveType - Maps a type name to its stream name. Throws when
 *   `typeAccessor` is not configured.
 * @param {function(string): string} resolveTag - Maps a tag value to its namespaced stream name
 *   (e.g. `'tags/' + tag`). Throws when `tagsAccessor` is not configured.
 * @returns {Array} Selector-algebra array (non-empty).
 * @throws {Error} When `items` is missing, null, or empty.
 */
function compileDcbQuery(query, resolveType, resolveTag) {
    if (!Array.isArray(query.items) || query.items.length === 0) {
        throw new Error("DcbQuery.items must be a non-empty array. Use ['_all'] for an unconstrained read.");
    }
    return query.items.map(item => compileDcbItem(item, resolveType, resolveTag));
}

/**
 * Compile a single DcbQuery item into a sub-selector.
 *
 * An item with no constraints (empty/missing `types` and `tags`) compiles to `'_all'`.
 * Explicit `null` on `types` or `tags` throws — it is almost always a programming error.
 *
 * @param {object} item - QueryItem with optional `types: string[]` and `tags: string[]`.
 * @param {function(string): string} resolveType
 * @param {function(string): string} resolveTag
 * @returns {string|Array} Sub-selector for this item.
 * @throws {Error} When item is null/undefined, or when `types`/`tags` is non-null and non-array.
 */
function compileDcbItem(item, resolveType, resolveTag) {
    if (item === null || item === undefined) {
        throw new Error('DcbQuery item must be an object, got ' + item);
    }
    if (item.tags !== undefined && !Array.isArray(item.tags)) {
        throw new Error('DcbQuery item.tags must be an array or undefined, got ' + typeof item.tags);
    }
    if (item.types !== undefined && !Array.isArray(item.types)) {
        throw new Error('DcbQuery item.types must be an array or undefined, got ' + typeof item.types);
    }

    const terms = [];

    for (const tag of item.tags ?? []) {
        terms.push(resolveTag(tag));
    }
    if (item.types?.length) {
        const typeStreams = item.types.map(resolveType);
        terms.push(typeStreams.length === 1 ? typeStreams[0] : typeStreams);
    }

    if (terms.length === 0) return '_all';
    if (terms.length === 1 && typeof terms[0] === 'string') return terms[0];
    return terms; // inner array = AND level
}

export {isDcbQuery, compileDcbQuery};
