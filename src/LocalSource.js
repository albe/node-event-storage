import { assert } from './utils/util.js';
import { matches } from './utils/metadataUtil.js';
import { ReadOnly as ReadOnlyStorage } from './Storage.js';

/**
 * @typedef {import('./Projection.js').Envelope} Envelope
 * @typedef {import('./Projection.js').Selection} Selection
 * @typedef {import('./Projection.js').EventSource} EventSource
 */

/**
 * @param {object} metadata Storage metadata of an event.
 * @returns {number} The global position of the event.
 */
function positionOf(metadata) {
    return metadata.commitId + metadata.commitVersion + 1;
}

/**
 * @param {object|function|null|undefined} matcher A commit condition matcher.
 * @param {{ payload: object, metadata: object }} document
 * @returns {boolean}
 */
function matchesMatcher(matcher, document) {
    if (!matcher) {
        return true;
    }
    return typeof matcher === 'function' ? matcher(document.payload, document.metadata) : matches(document, matcher);
}

/**
 * An event source over a local store, for decision models and read models.
 *
 * @param {import('./EventStore.js').default} eventStore A store configured with `typeAccessor` (and `tagsAccessor`
 *   when projections use a tagFilter).
 * @returns {EventSource}
 */
function localSource(eventStore) {
    assert(eventStore.typeSource !== null, 'Projections need the store to be configured with a typeAccessor.');
    const typeOf = eventStore.typeSource.accessor;
    const tagsOf = eventStore.tagsSource ? eventStore.tagsSource.accessor : () => [];

    /**
     * @param {{ payload: object, metadata: object }} document
     * @param {number} [position]
     * @returns {Envelope}
     */
    const envelope = (document, position = positionOf(document.metadata)) => ({
        event: document.payload,
        metadata: document.metadata,
        position,
        type: typeOf(document.payload),
        tags: tagsOf(document.payload) ?? []
    });

    /**
     * @param {Selection} selection
     * @param {number} from
     * @param {number} until
     * @returns {Generator<Envelope>}
     */
    const readRange = function* (selection, from, until) {
        if (from > until) {
            return;
        }
        const stream = eventStore.fromStreams('_source', selection.selector, from, until, selection.matcher ?? null);
        let document;
        while ((document = stream.next()) !== false) {
            yield envelope(document);
        }
    };

    /**
     * Whether a just written document belongs to the selection, using the matchers that route documents into the
     * stream indexes. Depth 0 is OR, odd depths are AND.
     *
     * @param {Array<string|Array>|string} selector
     * @param {object} document
     * @param {number} depth
     * @returns {boolean}
     */
    const selects = (selector, document, depth = 0) => {
        if (typeof selector === 'string') {
            if (selector === '_all') return true;
            const index = eventStore.storage.secondaryIndexes['stream-' + selector];
            return index !== undefined && matches(document, index.matcher);
        }
        return depth % 2 === 0
            ? selector.some(node => selects(node, document, depth + 1))
            : selector.every(node => selects(node, document, depth + 1));
    };

    return Object.freeze({
        async query(query, { from = 1 } = {}) {
            const { condition } = eventStore.query(query, null, from);
            return { events: toAsync(readRange(condition, from, condition.noneMatchAfter)), condition };
        },

        read(selection, { from = 1 } = {}) {
            return toAsync(readRange(selection, from, eventStore.length));
        },

        async *follow(selection, { from = 1, signal } = {}) {
            const storage = eventStore.storage;
            const eventName = storage instanceof ReadOnlyStorage ? 'wrote' : 'index-add';
            const written = [];
            let wake = null;
            const listener = eventName === 'wrote'
                ? (document, entry) => { written.push([entry.number, document]); wake?.(); }
                : (indexName, number, document) => { if (indexName === '_all') { written.push([number, document]); wake?.(); } };
            const onAbort = () => wake?.();
            storage.on(eventName, listener);
            signal?.addEventListener('abort', onAbort);
            try {
                let position = eventStore.length;
                for (const existing of readRange(selection, from, position)) {
                    if (signal?.aborted) return;
                    yield existing;
                }
                position = Math.max(position, from - 1);
                while (!signal?.aborted) {
                    if (written.length === 0) {
                        await new Promise(resolve => { wake = resolve; });
                        wake = null;
                        continue;
                    }
                    const [number, document] = written.shift();
                    if (number <= position) continue;
                    position = number;
                    if (selects(selection.selector, document) && matchesMatcher(selection.matcher, document)) {
                        yield envelope(document, number);
                    }
                }
            } finally {
                storage.removeListener(eventName, listener);
                signal?.removeEventListener('abort', onAbort);
            }
        }
    });
}

/**
 * @template T
 * @param {Iterable<T>} items
 * @returns {AsyncIterable<T>}
 */
async function* toAsync(items) {
    yield* items;
}

export { localSource };
