import { assert } from './utils/util.js';
import { matchesDcbQuery } from './utils/dcbUtil.js';

/**
 * @typedef {object} ProjectionDefinition
 * @property {any} initialState The state before any event was applied.
 * @property {Object<string, function(any, object, object): any>} handlers Pure reducers keyed by event type.
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
 * The query is derived from the handler keys (event types) and the tag filter.
 *
 * @param {ProjectionDefinition} definition
 * @returns {Projection}
 */
function createProjection({ initialState = null, handlers, tagFilter = [] }) {
    assert(handlers !== null && typeof handlers === 'object', 'A projection needs a handlers object.');
    const types = Object.keys(handlers);
    assert(types.length > 0, 'A projection needs at least one event handler.');
    assert(Array.isArray(tagFilter), 'Projection tagFilter must be an array of tags.');
    const query = { items: [tagFilter.length ? { types, tags: [...tagFilter] } : { types }] };
    return Object.freeze({
        initialState,
        query,
        apply(state, event, metadata, { type, tags }) {
            if (!matchesDcbQuery(query, type, tags)) return state;
            return handlers[type](state, event, metadata);
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
 * Build a decision model: read all events relevant to the projection(s) and fold them into state.
 * Returns the state together with the append condition that protects the decision.
 *
 * @param {import('./EventStore.js').default} eventStore A store configured with `typeAccessor` (and `tagsAccessor`
 *   when projections use a tagFilter).
 * @param {Projection|Object<string, Projection>} projectionOrMap A projection, or a map of projections to compose.
 * @returns {{ state: any, appendCondition: import('./EventStore.js').CommitCondition }}
 */
function buildDecisionModel(eventStore, projectionOrMap) {
    assert(eventStore.typeSource !== null, 'Projections need the store to be configured with a typeAccessor.');
    const projection = isProjection(projectionOrMap) ? projectionOrMap : composeProjections(projectionOrMap);
    const { stream, condition } = eventStore.query(projection.query);
    const typeOf = eventStore.typeSource.accessor;
    const tagsOf = eventStore.tagsSource ? eventStore.tagsSource.accessor : () => [];
    let state = projection.initialState;
    stream.forEach((event, metadata) => {
        const context = { type: typeOf(event), tags: tagsOf(event) ?? [] };
        state = projection.apply(state, event, metadata, context);
    });
    return { state, appendCondition: condition };
}

export { createProjection, composeProjections, buildDecisionModel };
