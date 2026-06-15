import {ExpectedVersion, isExpectedVersionMarker} from "../ExpectStream.js";

/**
 * Normalize commit overloads into a single argument object.
 *
 * @param {object|object[]} events Event or event list.
 * @param {number|object|function} expectedVersion Expected version marker, or already metadata/callback.
 * @param {object|function} metadata Commit metadata or callback.
 * @param {function} callback Completion callback.
 * @returns {{events: object[], expectedVersion: number|object, metadata: object, callback: function}} Normalized commit arguments.
 */
function normalizeCommitArgumentTypes(events, expectedVersion, metadata, callback) {
    if (!(events instanceof Array)) {
        events = [events];
    }
    if (typeof expectedVersion !== 'number' && !isExpectedVersionMarker(expectedVersion)) {
        callback = metadata;
        metadata = expectedVersion;
        expectedVersion = ExpectedVersion.Any;
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
 * Normalize query() overloads.
 *
 * Supports:
 * - query(types, matcher?, minSequenceNumber?, raw?)
 * - query(types, options)
 * - query(types, matcher, options)
 *
 * @param {object|function|null} matcherOrOptions Matcher function/object or options object.
 * @param {number|object} minSequenceNumber Positional minimum sequence number or options object.
 * @param {boolean} raw Positional raw flag.
 * @returns {{matcher: object|function|null, minSequenceNumber: number, raw: boolean}}
 */
function normalizeQueryArguments(matcherOrOptions, minSequenceNumber, raw) {
    if (minSequenceNumber && typeof minSequenceNumber === 'object' && !(minSequenceNumber instanceof Function)) {
        const options = minSequenceNumber;
        return {
            matcher: matcherOrOptions ?? options.matcher ?? null,
            minSequenceNumber: options.fromSequenceNumber ?? 1,
            raw: options.raw ?? false
        };
    }

    let matcher = matcherOrOptions;
    let queryOptions = null;
    if (matcherOrOptions && typeof matcherOrOptions === 'object' && typeof matcherOrOptions !== 'function') {
        const looksLikeOptions =
            matcherOrOptions.fromSequenceNumber !== undefined ||
            matcherOrOptions.toSequenceNumber !== undefined ||
            matcherOrOptions.raw !== undefined ||
            matcherOrOptions.matcher !== undefined;
        if (looksLikeOptions) {
            queryOptions = matcherOrOptions;
            matcher = queryOptions.matcher ?? null;
        }
    }

    if (queryOptions) {
        return {
            matcher,
            minSequenceNumber: queryOptions.fromSequenceNumber ?? 1,
            raw: queryOptions.raw ?? false
        };
    }
    return { matcher, minSequenceNumber, raw };
}

/**
 * Normalize legacy stream read arguments into a canonical options object with defaults.
 *
 * @param {number|object} fromOrOptions Positional `from` value or options object.
 * @param {number} to Positional `to` value.
 * @param {object|function|boolean|null} predicate Matcher/predicate or raw shorthand.
 * @param {boolean} raw Raw flag from the positional signature.
 * @param {'stream'|'sequence'} mode Positional interpretation mode.
 * @returns {{fromStreamVersion?: number, toStreamVersion?: number, fromSequenceNumber?: number, toSequenceNumber?: number, predicate: object|function|null, raw: boolean}}
 */
function normalizeStreamReadOptions(fromOrOptions, to, predicate, raw, mode) {
    if (typeof fromOrOptions === 'object' && fromOrOptions !== null && !(fromOrOptions instanceof Function)) {
        const options = fromOrOptions;
        ({ predicate, raw } = normalizePredicateRaw(options.predicate ?? null, options.raw ?? false));
        return {
            fromStreamVersion: options.fromStreamVersion,
            toStreamVersion: options.toStreamVersion,
            fromSequenceNumber: options.fromSequenceNumber,
            toSequenceNumber: options.toSequenceNumber,
            predicate,
            raw
        };
    }

    ({ predicate, raw } = normalizePredicateRaw(predicate, raw));
    if (mode === 'sequence') {
        return {
            fromSequenceNumber: fromOrOptions,
            toSequenceNumber: to,
            predicate,
            raw
        };
    }
    return {
        fromStreamVersion: fromOrOptions,
        toStreamVersion: to,
        predicate,
        raw
    };
}

/**
 * Parse category selector prefixes.
 *
 * Supports selectors ending with '-*' or '/*'.
 * Returns null when no category selector syntax is used.
 *
 * @param {string|string[]} streamName
 * @returns {string|null}
 */
function resolveCategorySelectorPrefix(streamName) {
    if (typeof streamName !== 'string') {
        return null;
    }
    if (streamName.endsWith('-*') || streamName.endsWith('/*')) {
        return streamName.slice(0, -2);
    }
    return null;
}

export {
    normalizeCommitArgumentTypes,
    parseStreamFromIndexName,
    normalizePredicateRaw,
    normalizeNamedCtorArgs,
    normalizeRevision,
    normalizeMaxRevision,
    normalizeConsumerStateArgs,
    normalizeQueryArguments,
    normalizeStreamReadOptions,
    resolveCategorySelectorPrefix
};



