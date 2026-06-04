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

function parseStreamFromIndexName(indexName) {
    if (indexName === '_all') {
        return '_all';
    }
    if (indexName.startsWith('stream-')) {
        return indexName.slice(7);
    }
    return indexName;
}

function normalizePredicateRaw(predicate, raw) {
    if (typeof predicate === 'boolean' && raw === false) {
        return { predicate: null, raw: predicate };
    }
    return { predicate, raw };
}

function normalizeNamedCtorArgs(name, options, fallbackName = undefined) {
    if (typeof name !== 'string') {
        return { name: fallbackName, options: name };
    }
    return { name, options };
}

function normalizeRevision(version, length) {
    return version < 0 ? version + length + 1 : version;
}

function normalizeMaxRevision(length, maxRevision) {
    return Math.min(length, maxRevision < 0 ? length + maxRevision + 1 : maxRevision);
}

function normalizeConsumerStateArgs(initialState, startFrom) {
    if (typeof initialState === 'number') {
        return { initialState: {}, startFrom: initialState };
    }
    return { initialState, startFrom };
}

export {
    fixCommitArgumentTypes,
    parseStreamFromIndexName,
    normalizePredicateRaw,
    normalizeNamedCtorArgs,
    normalizeRevision,
    normalizeMaxRevision,
    normalizeConsumerStateArgs
};



