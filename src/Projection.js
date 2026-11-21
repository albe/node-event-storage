import { assert } from './utils/util.js';
import { matchesDcbQuery } from './utils/dcbUtil.js';

/**
 * @typedef {object} ProjectionDefinition
 * @property {any} initialState The state before any event was applied.
 * @property {Object<string, function(any, object, object): any>} when Pure reducers keyed by event type.
 *   Each receives `(state, event, metadata)` and returns the new state.
 * @property {string[]} [tagFilter] Tags that every handled event must carry (AND semantics).
 */

/**
 * @typedef {object} Projection
 * @property {any} initialState
 * @property {{ items: Array<{ types?: string[], tags?: string[] }> }} query The DcbQuery selecting all events this
 *   projection needs.
 * @property {function(any, object, object, {type: string, tags: string[]}): any} apply Apply one event. Events that
 *   do not match `query` leave the state unchanged.
 */

/**
 * Create a projection in the dcb.events declarative format.
 * The query is derived from the event types in `when` and the tag filter.
 *
 * @param {ProjectionDefinition} definition
 * @returns {Projection}
 */
function createProjection({ initialState = null, when, tagFilter = [] }) {
    assert(when !== null && typeof when === 'object', 'A projection needs a `when` object with one handler per event type.');
    const types = Object.keys(when);
    assert(types.length > 0, 'A projection needs at least one event handler.');
    assert(Array.isArray(tagFilter), 'Projection tagFilter must be an array of tags.');
    const query = { items: [tagFilter.length ? { types, tags: [...tagFilter] } : { types }] };
    return Object.freeze({
        initialState,
        query,
        apply(state, event, metadata, { type, tags }) {
            if (!matchesDcbQuery(query, type, tags)) return state;
            return when[type](state, event, metadata);
        }
    });
}

/**
 * Compose several projections into one. The state is an object with one key per projection, the query
 * is the OR of all sub-queries, and each event is only applied to the projections whose query it matches.
 *
 * @param {Object<string, Projection>} projections
 * @returns {Projection}
 */
function composeProjections(projections) {
    const entries = Object.entries(projections);
    assert(entries.length > 0, 'Need at least one projection to compose.');
    const initialState = Object.fromEntries(entries.map(([key, p]) => [key, p.initialState]));
    const query = { items: entries.flatMap(([, p]) => p.query.items) };
    return Object.freeze({
        initialState,
        query,
        apply(state, event, metadata, context) {
            const next = { ...state };
            for (const [key, projection] of entries) {
                next[key] = projection.apply(state[key], event, metadata, context);
            }
            return next;
        }
    });
}

/**
 * @param {any} value
 * @returns {boolean} True if the value looks like a single projection rather than a map of projections.
 */
function isProjection(value) {
    return value !== null && typeof value === 'object' && typeof value.apply === 'function' && 'query' in value;
}

/**
 * @typedef {object} Envelope One event as delivered by an {@link EventSource}.
 * @property {object} event The event payload.
 * @property {object} metadata The storage metadata.
 * @property {number} position The global position of the event.
 * @property {string} type The event type, read from the stored event.
 * @property {string[]} tags The event tags, read from the stored event.
 */

/**
 * @typedef {object} Selection The events to read: a compiled selector and an optional matcher, e.g. a commit condition.
 * @property {Array<string|Array>} selector
 * @property {object|function|null} [matcher]
 */

/**
 * @typedef {object} EventSource The read side that projections need from a store. Implemented by `localSource(store)`
 *   in `event-storage` and by `httpSource(client)` in `event-storage-http`.
 * @property {function(object, {from?: number}=): Promise<{events: AsyncIterable<Envelope>, condition: object}>} query
 *   Read the events matching a DcbQuery from a global position, together with the commit condition at the time of the
 *   query. Only events up to the condition's position are delivered.
 * @property {function(Selection, {from?: number}=): AsyncIterable<Envelope>} read Read the events of a selection from
 *   a global position up to the end of the store.
 * @property {function(Selection, {from?: number, signal?: AbortSignal}=): AsyncIterable<Envelope>} follow Read the
 *   events of a selection from a global position and keep delivering new ones until the signal aborts.
 */

/**
 * @param {any} source
 */
function assertSource(source) {
    assert(source !== null && typeof source === 'object'
        && typeof source.query === 'function' && typeof source.read === 'function' && typeof source.follow === 'function',
    'Projections read from an event source. Pass localSource(store) or httpSource(client).');
}

/**
 * The state of one or more projections at a known position of the store, together with the append condition
 * that protects a decision made on it. Immutable: {@link DecisionModel#update} returns a new model.
 */
class DecisionModel {

    /**
     * @param {EventSource} source
     * @param {Projection} projection
     * @param {any} state
     * @param {object} commitCondition The condition returned by the source.
     */
    constructor(source, projection, state, commitCondition) {
        this.source = source;
        this.projection = projection;
        this.state = state;
        this.commitCondition = commitCondition;
        Object.freeze(this);
    }

    /**
     * The events matching the condition that were committed after this model was built, i.e. the events that
     * make a commit with that condition fail. Defaults to this model's `commitCondition`; pass a narrowed
     * condition (e.g. from `commitCondition.without(...)`) to get only the events that conflict with it.
     *
     * @param {object} [condition]
     * @returns {AsyncIterable<Envelope>}
     */
    since(condition = this.commitCondition) {
        return this.source.read(condition, { from: condition.noneMatchAfter + 1 });
    }

    /**
     * Apply the events committed since this model was built and return the updated model. Only those events are
     * read. Resolves to this model when nothing new was committed.
     *
     * @returns {Promise<DecisionModel>}
     */
    async update() {
        const model = await readDecisionModel(this.source, this.projection, this.state, this.commitCondition.noneMatchAfter + 1);
        return model.commitCondition.noneMatchAfter === this.commitCondition.noneMatchAfter ? this : model;
    }
}

/**
 * @param {EventSource} source
 * @param {Projection} projection
 * @param {any} state The state to apply the events to.
 * @param {number} from The first global position to read.
 * @returns {Promise<DecisionModel>}
 */
async function readDecisionModel(source, projection, state, from) {
    const { events, condition } = await source.query(projection.query, { from });
    for await (const envelope of events) {
        state = applyEnvelope(projection, state, envelope);
    }
    return new DecisionModel(source, projection, state, condition);
}

/**
 * Apply one event to the projection.
 *
 * @param {Projection} projection
 * @param {any} state
 * @param {Envelope} envelope
 * @returns {any} The new state.
 */
function applyEnvelope(projection, state, { event, metadata, type, tags }) {
    return projection.apply(state, event, metadata, { type, tags });
}

/**
 * @param {Projection|Object<string, Projection>} projectionOrMap
 * @returns {Projection} The projection, composed when a map was given.
 */
function resolveProjection(projectionOrMap) {
    return isProjection(projectionOrMap) ? projectionOrMap : composeProjections(projectionOrMap);
}

/**
 * Build a decision model: read all events relevant to the projection(s) and fold them into state.
 * The returned model carries the append condition that protects the decision. After a conflict, or to keep
 * a model in memory across commands, call `model.update()` to only apply the events committed since.
 *
 * @param {EventSource} source Where to read the events from, e.g. `localSource(store)` or `httpSource(client)`.
 * @param {Projection|Object<string, Projection>} projectionOrMap A projection, or a map of projections to compose.
 * @returns {Promise<DecisionModel>}
 */
async function buildDecisionModel(source, projectionOrMap) {
    assertSource(source);
    const projection = resolveProjection(projectionOrMap);
    return readDecisionModel(source, projection, projection.initialState, 1);
}

/**
 * Wrap a source so that every event passes through `transform` before it reaches a projection, e.g. to decrypt
 * fields. Type and tags are read from the stored event before the transformation, because the store selects
 * events by them. Matchers in a commit condition also see the stored, untransformed event.
 *
 * @param {EventSource} source
 * @param {function(object, object): (object|Promise<object>)} transform Receives `(event, metadata)` and returns
 *   the event the projections see.
 * @returns {EventSource}
 */
function transformEvents(source, transform) {
    assertSource(source);
    assert(typeof transform === 'function', 'transformEvents() needs a transform function.');
    const map = async function* (envelopes) {
        for await (const envelope of envelopes) {
            yield { ...envelope, event: await transform(envelope.event, envelope.metadata) };
        }
    };
    return Object.freeze({
        ...source,
        async query(query, options) {
            const { events, condition } = await source.query(query, options);
            return { events: map(events), condition };
        },
        read: (selection, options) => map(source.read(selection, options)),
        follow: (selection, options) => map(source.follow(selection, options))
    });
}

export { createProjection, composeProjections, buildDecisionModel, transformEvents, DecisionModel, applyEnvelope, resolveProjection, assertSource, readDecisionModel };
