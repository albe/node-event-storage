import type { CommitCondition, DcbQuery, EventStore, EventStream } from './index.js';

/** Pure reducer for one event type. Must not have side effects. */
export type ProjectionHandler<S, E = any> = (state: S, event: E, metadata: Record<string, any>) => S;

export interface ProjectionDefinition<S> {
    initialState: S;
    /** Reducers keyed by event type. The keys are the event types the projection reads. */
    when: Record<string, ProjectionHandler<S>>;
    /** Tags every handled event must carry (AND). */
    tagFilter?: string[];
}

export interface Projection<S> {
    readonly initialState: S;
    /** The DcbQuery selecting all events this projection needs. */
    readonly query: DcbQuery;
    /** Apply one event. Events that do not match `query` return `state` unchanged. */
    apply(state: S, event: any, metadata: Record<string, any>, context: { type: string; tags: string[] }): S;
}

export type ProjectionState<P> = P extends Projection<infer S> ? S : never;
export type ComposedState<M extends Record<string, Projection<any>>> = { [K in keyof M]: ProjectionState<M[K]> };

export function createProjection<S>(definition: ProjectionDefinition<S>): Projection<S>;

export function composeProjections<M extends Record<string, Projection<any>>>(projections: M): Projection<ComposedState<M>>;

/** Projection state at a known store position plus the condition protecting a decision on it. Immutable. */
export class DecisionModel<S> {
    readonly state: S;
    readonly commitCondition: CommitCondition;
    /** Events matching `condition` (default: `commitCondition`) committed after this model was built, i.e. those that block a commit. */
    since(condition?: CommitCondition): EventStream;
    /** Apply only the events committed since this model was built. Returns this model when nothing is new. */
    update(): DecisionModel<S>;
}

export function buildDecisionModel<S>(eventStore: EventStore, projection: Projection<S>): DecisionModel<S>;
export function buildDecisionModel<M extends Record<string, Projection<any>>>(eventStore: EventStore, projections: M): DecisionModel<ComposedState<M>>;
