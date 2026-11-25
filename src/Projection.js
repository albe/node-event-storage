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
 * The state of one or more projections at a known position of the store, together with the append condition
 * that protects a decision made on it. Immutable: {@link DecisionModel#update} returns a new model.
 */
class DecisionModel {

    /**
     * @param {import('./EventStore.js').default} eventStore
     * @param {Projection} projection
     * @param {any} state
     * @param {import('./EventStore.js').CommitCondition} commitCondition
     */
    constructor(eventStore, projection, state, commitCondition) {
        this.eventStore = eventStore;
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
     * @param {import('./EventStore.js').CommitCondition} [condition]
     * @returns {import('./EventStream.js').default}
     */
    since(condition = this.commitCondition) {
        return this.eventStore.fromStreams(
            '_since_' + condition.noneMatchAfter,
            condition.selector,
            condition.noneMatchAfter + 1,
            -1,
            condition.matcher,
            condition.raw
        );
    }

    /**
     * Apply the events committed since this model was built and return the updated model. Only those events are
     * read. Returns this model unchanged when nothing new was committed.
     *
     * @returns {DecisionModel}
     */
    update() {
        if (this.eventStore.length === this.commitCondition.noneMatchAfter) {
            return this;
        }
        return readDecisionModel(this.eventStore, this.projection, this.state, this.commitCondition.noneMatchAfter + 1);
    }
}

/**
 * @param {import('./EventStore.js').default} eventStore
 * @param {Projection} projection
 * @param {any} state The state to apply the events to.
 * @param {number} from The first global position to read.
 * @returns {DecisionModel}
 */
function readDecisionModel(eventStore, projection, state, from) {
    const { stream, condition } = eventStore.query(projection.query, null, from);
    const typeOf = eventStore.typeSource.accessor;
    const tagsOf = eventStore.tagsSource ? eventStore.tagsSource.accessor : () => [];
    stream.forEach((event, metadata) => {
        const context = { type: typeOf(event), tags: tagsOf(event) ?? [] };
        state = projection.apply(state, event, metadata, context);
    });
    return new DecisionModel(eventStore, projection, state, condition);
}

/**
 * Build a decision model: read all events relevant to the projection(s) and fold them into state.
 * The returned model carries the append condition that protects the decision. After a conflict, or to keep
 * a model in memory across commands, call `model.update()` to only apply the events committed since.
 *
 * @param {import('./EventStore.js').default} eventStore A store configured with `typeAccessor` (and `tagsAccessor`
 *   when projections use a tagFilter).
 * @param {Projection|Object<string, Projection>} projectionOrMap A projection, or a map of projections to compose.
 * @returns {DecisionModel}
 */
function buildDecisionModel(eventStore, projectionOrMap) {
    assert(eventStore.typeSource !== null, 'Projections need the store to be configured with a typeAccessor.');
    const projection = isProjection(projectionOrMap) ? projectionOrMap : composeProjections(projectionOrMap);
    return readDecisionModel(eventStore, projection, projection.initialState, 1);
}

export { createProjection, composeProjections, buildDecisionModel, DecisionModel };
