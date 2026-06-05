import { assert } from './utils/util.js';
import { buildMatcherFromMetadata, buildMetadataForMatcher } from './utils/metadataUtil.js';

/**
 * Build the CompositeProjection class on top of the provided Projection base class.
 * This avoids cyclic imports between Projection and CompositeProjection modules.
 *
 * @param {typeof import('./Projection.js').default} Projection
 * @returns {typeof import('./Projection.js').CompositeProjection}
 */
function createCompositeProjectionClass(Projection) {
    return class CompositeProjection extends Projection {

        /**
         * @param {string} name
         * @param {object<string, Projection|object>} projections
         * @param {{ matcher?: object|function(object): boolean, hmac?: function(string): string, typeAccessor?: function(object): string }} [options]
         */
        constructor(name, projections, options = {}) {
            assert(projections && typeof projections === 'object' && !Array.isArray(projections), 'CompositeProjection requires an object map of projections.');
            const normalized = {};
            for (const [projectionName, projection] of Object.entries(projections)) {
                normalized[projectionName] = projection instanceof Projection
                    ? projection
                    : new Projection(projectionName, projection, options);
            }
            super(name, {
                initialState: Object.fromEntries(
                    Object.entries(normalized).map(([projectionName, projection]) => [projectionName, projection.initialState])
                ),
                handlers: (state) => state,
                matcher: options.matcher
            }, options);
            this.projections = normalized;
            this.reset();
        }

        get types() {
            const types = new Set();
            for (const projection of Object.values(this.projections)) {
                for (const type of projection.types) {
                    types.add(type);
                }
            }
            return [...types];
        }

        /**
         * Apply one event across all child projections and return composed state.
         * @param {object} state
         * @param {object} event
         * @returns {object}
         */
        apply(state, event) {
            if (!this.matches(event)) {
                this.state = state;
                return state;
            }
            const currentState = state || this.initialState;
            const nextState = {};
            for (const [name, projection] of Object.entries(this.projections)) {
                nextState[name] = projection.apply(currentState[name], event);
            }
            this.state = nextState;
            return nextState;
        }

        /**
         * Reset all child projections and rebuild composed state.
         * @returns {object}
         */
        reset() {
            for (const projection of Object.values(this.projections)) {
                projection.reset();
            }
            this.state = Object.fromEntries(
                Object.entries(this.projections).map(([projectionName, projection]) => [projectionName, projection.state])
            );
            return this.state;
        }

        /**
         * Serialize composed projection metadata recursively.
         * @param {function(string): string} [hmac]
         * @returns {object}
         */
        toMetadata(hmac = this.hmac) {
            return {
                kind: 'composite-projection',
                name: this.name,
                matcher: this.matcher ? buildMetadataForMatcher(this.matcher, hmac) : null,
                projections: Object.fromEntries(
                    Object.entries(this.projections).map(([name, projection]) => [name, projection.toMetadata(hmac)])
                )
            };
        }

        /**
         * Restore a composed projection from serialized metadata.
         * @param {object} metadata
         * @param {{ matcher?: object|function(object): boolean, hmac?: function(string): string, typeAccessor?: function(object): string }} [options]
         * @returns {CompositeProjection}
         */
        static fromMetadata(metadata, options = {}) {
            const hmac = options.hmac;
            const deserializeMatcher = (matcherMetadata) => {
                if (!matcherMetadata) {
                    return undefined;
                }
                if (typeof matcherMetadata.matcher === 'string') {
                    assert(typeof hmac === 'function', 'Must provide options.hmac to restore function projections.');
                }
                return buildMatcherFromMetadata(matcherMetadata, hmac);
            };
            const projections = Object.fromEntries(
                Object.entries(metadata.projections || {}).map(([name, projectionMetadata]) => [
                    name,
                    Projection.fromMetadata(projectionMetadata, options)
                ])
            );
            return new this(metadata.name, projections, {
                ...options,
                matcher: deserializeMatcher(metadata.matcher)
            });
        }
    };
}

export { createCompositeProjectionClass };
