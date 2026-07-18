/**
 * Normalize commit overloads into a single argument object.
 *
 * @param {object|object[]} events Event or event list.
 * @param {number|object|function} expectedVersion Expected version, CommitCondition, or already metadata/callback.
 * @param {object|function} metadata Commit metadata or callback.
 * @param {function} callback Completion callback.
 * @param {number} ExpectedVersionAny Fallback value for "any" expectedVersion.
 * @param {Function} CommitConditionClass Class used for CommitCondition checks.
 * @returns {{events: object[], expectedVersion: number|object, metadata: object, callback: function}} Normalized commit arguments.
 */
function fixCommitArgumentTypes(events, expectedVersion, metadata, callback, ExpectedVersionAny, CommitConditionClass) {
    if (!(events instanceof Array)) {
        events = [events];
    }
    if (typeof expectedVersion !== 'number' && !(expectedVersion instanceof CommitConditionClass)) {
        callback = metadata;
        metadata = expectedVersion;
        expectedVersion = ExpectedVersionAny;
    }
    if (typeof metadata !== 'object') {
        callback = metadata;
        metadata = {};
    }
    if (typeof callback !== 'function') {
        callback = () => {};
    }
    return { events, expectedVersion, metadata, callback };
}

/**
 * Derive the stream name from an index name.
 *
 * @param {string} indexName Index file/index name.
 * @returns {string} Corresponding stream name.
 */
function parseStreamFromIndexName(indexName) {
    if (indexName === '_all') {
        return '_all';
    }
    if (indexName.startsWith('stream-')) {
        return indexName.slice(7);
    }
    return indexName;
}

/**
 * Support predicate/raw shorthand (`predicate=true`).
 *
 * @param {object|function|boolean|null} predicate Filter predicate or raw shorthand.
 * @param {boolean} raw Raw flag from the call signature.
 * @returns {{predicate: object|function|null, raw: boolean}} Normalized predicate/raw pair.
 */
function normalizePredicateRaw(predicate, raw) {
    if (typeof predicate === 'boolean' && raw === false) {
        return { predicate: null, raw: predicate };
    }
    return { predicate, raw };
}

/**
 * Support constructor overloads with optional `name`.
 *
 * @param {string|object} name Name or already the options object.
 * @param {object} options Options object when `name` is provided.
 * @param {string|undefined} [fallbackName=undefined] Fallback name when no explicit `name` is passed.
 * @returns {{name: string|undefined, options: object}} Normalized name/options pair.
 */
function normalizeNamedCtorArgs(name, options, fallbackName = undefined) {
    if (typeof name !== 'string') {
        return { name: fallbackName, options: name };
    }
    return { name, options };
}

/**
 * Normalize negative revisions relative to stream length.
 *
 * @param {number} version Requested revision.
 * @param {number} length Current stream length.
 * @returns {number} Resolved revision.
 */
function normalizeRevision(version, length) {
    return version < 0 ? version + length + 1 : version;
}

/**
 * Clamp and normalize maxRevision, including negative values.
 *
 * @param {number} length Current stream length.
 * @param {number} maxRevision Requested max revision.
 * @returns {number} Effective max revision in valid range.
 */
function normalizeMaxRevision(length, maxRevision) {
    return Math.min(length, maxRevision < 0 ? length + maxRevision + 1 : maxRevision);
}

/**
 * Support the consumer overload where the first argument is a numeric start offset.
 *
 * @param {object|number} initialState Initial state or numeric start offset.
 * @param {number} startFrom Start offset from the call signature.
 * @returns {{initialState: object, startFrom: number}} Normalized consumer initialization values.
 */
function normalizeConsumerStateArgs(initialState, startFrom) {
    if (typeof initialState === 'number') {
        return { initialState: {}, startFrom: initialState };
    }
    return { initialState, startFrom };
}

/**
 * Create an object with a lazily-resolved property.
 *
 * @param {object} initialValues Eager properties to copy to the object.
 * @param {string} propertyName Property to resolve lazily.
 * @param {function(): *} resolver Function returning the property value on first access.
 * @returns {object}
 */
function createLazyPropertyHolder(initialValues, propertyName, resolver) {
    const holder = { ...initialValues };
    let resolved = false;
    let value = null;
    Object.defineProperty(holder, propertyName, {
        enumerable: true,
        configurable: true,
        get: () => {
            if (!resolved) {
                value = resolver();
                resolved = true;
            }
            return value;
        },
        set: (newValue) => {
            value = newValue;
            resolved = true;
        }
    });
    return holder;
}

export {
    fixCommitArgumentTypes,
    parseStreamFromIndexName,
    normalizePredicateRaw,
    normalizeNamedCtorArgs,
    normalizeRevision,
    normalizeMaxRevision,
    normalizeConsumerStateArgs,
    createLazyPropertyHolder
};


