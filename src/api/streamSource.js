/**
 * Internal helpers shared by the fluent query layer (EventStreamQuery / RangeQuery /
 * EventStreamResult). These translate a high-level source descriptor into the
 * underlying JoinEventStream primitives (via `store.fromStreams`, which is now the
 * canonical graceful entry point — missing streams are treated as empty), and
 * normalise the yielded storage documents into the public StoredEvent shape.
 *
 * Position model:
 *   - Global position: 1-based position in the global event log, equal to
 *     `commitId + commitVersion + 1`. Used by all sources except a single-stream
 *     `stream()` addressed via `.streamRange()`.
 *   - Stream-relative revision: 1-based position within a single physical stream.
 *     Only reachable via `stream(name).streamRange()`.
 */

/**
 * @typedef {string | SelectorNode[]} SelectorNode
 * @typedef {{ kind: 'stream', name: string }
 *   | { kind: 'category', name: string }
 *   | { kind: 'join', streams: SelectorNode[] }
 *   | { kind: 'all' }} StreamSource
 */

import { normalizeSelector } from '../utils/indexUtil.js';

/**
 * Recursively flatten a nested selector to its leaf stream names.
 *
 * @param {SelectorNode[]} selector
 * @returns {string[]}
 */
function flattenSelector(selector) {
    const names = [];
    for (const node of selector) {
        if (typeof node === 'string') {
            names.push(node);
        } else {
            names.push(...flattenSelector(node));
        }
    }
    return names;
}

/**
 * Normalize a raw storage document into the public StoredEvent shape.
 *
 * @param {{ stream: string, payload: object, metadata: object }} doc
 * @param {(function(object): string)|null} typeAccessor
 * @returns {object}
 */
function toStoredEvent(doc, typeAccessor) {
    const metadata = doc.metadata || {};
    const globalPosition = (metadata.commitId ?? 0) + (metadata.commitVersion ?? 0) + 1;
    return {
        stream: doc.stream,
        type: typeAccessor ? typeAccessor(doc.payload) : metadata.type,
        payload: doc.payload,
        metadata,
        globalPosition,
        streamVersion: (metadata.streamVersion ?? 0) + 1
    };
}

/**
 * Resolve a source descriptor to the flat list of currently-existing stream names
 * it covers. Used to short-circuit null returns when no matching streams exist yet,
 * and to compute transient stream labels.
 *
 * Missing streams are intentionally excluded here (empty iteration), but the OCC
 * condition is built separately via `conditionTypes`, which keeps the originally-
 * requested names regardless of current existence.
 *
 * @param {import('../EventStore.js').default} store
 * @param {StreamSource} source
 * @returns {string[]}
 */
function resolveStreamNames(store, source) {
    switch (source.kind) {
    case 'all':
        return ['_all'];
    case 'stream':
        return source.name in store.streams ? [source.name] : [];
    case 'join':
        return flattenSelector(source.streams).filter(name => name in store.streams);
    case 'category':
        if (source.name in store.streams) {
            return [source.name];
        }
        return Object.keys(store.streams).filter(name =>
            name.startsWith(source.name + '-') || name.startsWith(source.name + '/')
        );
    /* c8 ignore next 2 */
    default:
        throw new Error(`Unknown source kind: ${source.kind}`);
    }
}

/**
 * The stream names that make up the OCC `CommitCondition` for a source.
 *
 * For `stream` the single name is returned.
 * For `join` the **normalized** selector is returned (not flattened to leaves) so
 * the `CommitCondition` carries the full algebra, which `checkCondition` can re-use
 * directly without re-normalizing.
 * For `category` the concrete members are resolved at query time (open-ended
 * categories cannot include future members in the condition).
 *
 * @param {import('../EventStore.js').default} store
 * @param {StreamSource} source
 * @returns {string|SelectorNode[]}
 */
function conditionSelector(store, source) {
    switch (source.kind) {
    case 'all':
        return ['_all'];
    case 'stream':
        return source.name;
    case 'join':
        return normalizeSelector(source.streams);
    case 'category':
        return resolveStreamNames(store, source);
    /* c8 ignore next 2 */
    default:
        throw new Error(`Unknown source kind: ${source.kind}`);
    }
}

/**
 * A short, stable label for the transient joined-stream name used by
 * `store.fromStreams(...)`.
 *
 * @param {StreamSource} source
 * @param {string[]} names Pre-computed flat resolved names (avoids re-flattening).
 * @returns {string}
 */
function sourceLabel(source) {
    switch (source.kind) {
    case 'all':
        return '_q_all';
    case 'stream':
        return '_q_stream_' + source.name;
    case 'join':
        return '_q_join_' + Date.now();
    case 'category':
        return '_q_category_' + source.name;
    /* c8 ignore next 2 */
    default:
        throw new Error(`Unknown source kind: ${source.kind}`);
    }
}

/**
 * Build a global-position stream for the given source and bounds.
 *
 * All source kinds route through `store.fromStreams`, which is the canonical
 * graceful entry point: missing streams are treated as empty rather than thrown.
 * For `join` sources the original (possibly-nested algebra) selector is passed
 * through intact so AND/OR semantics are preserved; the flat `names` are used
 * only for the transient label.
 *
 * Returns `null` when no matching streams exist — callers treat `null` as an
 * empty iterator.
 *
 * @param {import('../EventStore.js').default} store
 * @param {StreamSource} source
 * @param {object|function|null} matcher
 * @param {number} from Inclusive global lower bound (or higher for backwards).
 * @param {number} until Inclusive global upper bound.
 * @param {boolean} [raw=false]
 * @returns {import('../EventStream.js').default|null}
 */
function buildGlobalStream(store, source, matcher, from, until, raw = false) {
    const names = resolveStreamNames(store, source);
    if (names.length === 0) {
        return null;
    }
    // For join sources normalize the selector once before passing to fromStreams,
    // so redundant / equivalent sub-selectors take the fast path.
    let selector;
    if (source.kind === 'join') {
        const n = normalizeSelector(source.streams);
        selector = Array.isArray(n) ? n : [n];
    } else {
        selector = names;
    }
    return store.fromStreams(sourceLabel(source), selector, from, until, matcher, raw);
}

/**
 * Build a stream-relative stream (single physical stream, version-addressed).
 *
 * @param {import('../EventStore.js').default} store
 * @param {string} name
 * @param {object|function|null} matcher
 * @param {number} from Inclusive lower revision (or higher for backwards).
 * @param {number} until Inclusive upper revision.
 * @param {boolean} [raw=false]
 * @returns {import('../EventStream.js').default|false}
 */
function buildRelativeStream(store, name, matcher, from, until, raw = false) {
    return store.getEventStream(name, from, until, matcher, raw);
}

/**
 * Synchronously iterate a built stream, yielding StoredEvents or raw buffers.
 *
 * @param {import('../EventStream.js').default|null|false} stream
 * @param {(function(object): string)|null} typeAccessor
 * @param {boolean} [raw=false]
 * @yields {object|Buffer}
 */
function* iterateStream(stream, typeAccessor, raw = false) {
    if (!stream) {
        return;
    }
    let next;
    while ((next = stream.next()) !== false) {
        yield raw ? stream.toRawBuffer(next) : toStoredEvent(next, typeAccessor);
    }
}

export {
    toStoredEvent,
    flattenSelector,
    resolveStreamNames,
    conditionSelector,
    sourceLabel,
    buildGlobalStream,
    buildRelativeStream,
    iterateStream
};
