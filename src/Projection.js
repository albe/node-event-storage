import fs from 'fs';
import path from 'path';
import { assert } from './utils/util.js';
import { ensureDirectory, writeFileAtomic } from './utils/fsUtil.js';
import { buildMatcherFromMetadata, buildMetadataForMatcher, matches } from './utils/metadataUtil.js';

const DEFAULT_TYPE_ACCESSOR = (event) => event?.type || event?.payload?.type;


class Projection {

    /**
     * @param {string} name Projection name.
     * @param {{ initialState?: object, handlers: function(object, object): object|object, matcher?: object|function(object): boolean }} [definition]
     * @param {{ hmac?: function(string): string, typeAccessor?: function(object): string, fileName?: string }} [options]
     */
    constructor(name, definition = {}, options = {}) {
        assert(typeof name === 'string' && name !== '', 'Projection must have a name.');
        const { initialState = {}, handlers, matcher } = definition;
        assert((typeof handlers === 'function') || (handlers && typeof handlers === 'object' && !Array.isArray(handlers)), 'Projection handlers must be a function or an object map of functions.');
        if (typeof handlers === 'object') {
            for (const reducer of Object.values(handlers)) {
                assert(typeof reducer === 'function', 'Projection handler maps must contain reducer functions.');
            }
        }
        this.name = name;
        this.initialState = Object.freeze(initialState);
        this.handlers = handlers;
        this.matcher = matcher;
        this.hmac = options.hmac || null;
        this.typeAccessor = options.typeAccessor || DEFAULT_TYPE_ACCESSOR;
        this.fileName = options.fileName || null;
        this.state = this.initialState;
    }

    get types() {
        if (typeof this.handlers === 'function') {
            return [];
        }
        return Object.keys(this.handlers);
    }

    /**
     * Apply one event to the provided state and return the next state.
     * @param {*} state
     * @param {object} event
     * @returns {*}
     */
    apply(state, event) {
        if (!this.matches(event)) {
            this.state = state;
            return state;
        }
        let reducer = this.handlers;
        if (typeof this.handlers === 'object') {
            reducer = this.handlers[this.typeAccessor(event)];
            if (typeof reducer !== 'function') {
                this.state = state;
                return state;
            }
        }
        const nextState = reducer(state, event);
        this.state = nextState;
        return nextState;
    }

    /**
     * Reset to initialState and project all events from the given iterable stream.
     * @param {Iterable<object>} stream
     * @returns {*}
     */
    handle(stream) {
        this.reset();
        for (const event of stream) {
            this.state = this.apply(this.state, event);
        }
        return this.state;
    }

    /**
     * Reset current projection state to its initial state.
     * @returns {*}
     */
    reset() {
        this.state = this.initialState;
        return this.state;
    }

    /**
     * Check whether an event matches this projection's matcher definition.
     * @param {object} event
     * @returns {boolean}
     */
    matches(event) {
        if (!this.matcher) {
            return true;
        }
        if (typeof this.matcher === 'function') {
            return this.matcher(event);
        }
        return matches(event, this.matcher);
    }

    /**
     * Subscribe this projection to a consumer and persist when needed.
     * @param {{ project: function(Projection): object }} consumer
     * @returns {Projection}
     */
    subscribe(consumer) {
        assert(consumer && typeof consumer.project === 'function', 'Projection.subscribe expects a Consumer instance.');
        consumer.project(this);
        return this;
    }

    /**
     * Persist projection definition metadata to disk.
     * @param {{ hmac?: function(string): string, fileName?: string }} [options]
     * @returns {string} Persisted file name.
     */
    persist(options = {}) {
        const hmac = options.hmac || this.hmac;
        const fileName = options.fileName || this.fileName || `${this.name}.projection`;
        const metadata = this.toMetadata(hmac);
        const tmpFile = fileName + '.tmp';
        ensureDirectory(path.dirname(fileName));
        writeFileAtomic(fileName, JSON.stringify(metadata), {
            tmpFileName: tmpFile,
            encoding: 'utf8'
        });
        this.fileName = fileName;
        this.hmac = hmac;
        return fileName;
    }

    /**
     * Serialize this projection definition into trusted metadata.
     * @param {function(string): string} [hmac]
     * @returns {object}
     */
    toMetadata(hmac = this.hmac) {
        const serializeFn = (fn) => {
            assert(typeof hmac === 'function', 'Must provide options.hmac for function projections.');
            return buildMetadataForMatcher(fn, hmac);
        };
        const matcherMetadata = this.matcher ? (
            typeof this.matcher === 'function' ? serializeFn(this.matcher) : buildMetadataForMatcher(this.matcher, hmac)
        ) : null;
        const handlersMetadata = (typeof this.handlers === 'function')
            ? serializeFn(this.handlers)
            : Object.fromEntries(
                Object.entries(this.handlers).map(([eventType, reducer]) => [eventType, serializeFn(reducer)])
            );
        return {
            kind: 'projection',
            name: this.name,
            initialState: this.initialState,
            matcher: matcherMetadata,
            handlersKind: typeof this.handlers === 'function' ? 'function' : 'map',
            handlers: handlersMetadata
        };
    }

    /**
     * Restore a projection by name from default or configured file location.
     * @param {string} name
     * @param {{ fileName?: string, hmac?: function(string): string, typeAccessor?: function(object): string }} [options]
     * @returns {Projection}
     */
    static restore(name, options = {}) {
        assert(typeof name === 'string' && name !== '', 'Projection.restore requires a projection name.');
        const fileName = options.fileName || `${name}.projection`;
        return Projection.restoreFromFile(fileName, options);
    }

    /**
     * Restore a projection from an explicit metadata file path.
     * @param {string} fileName
     * @param {{ hmac?: function(string): string, typeAccessor?: function(object): string }} [options]
     * @returns {Projection}
     */
    static restoreFromFile(fileName, options = {}) {
        assert(fs.existsSync(fileName), `Projection file does not exist: ${fileName}`);
        const metadata = JSON.parse(fs.readFileSync(fileName, 'utf8'));
        return Projection.fromMetadata(metadata, { ...options, fileName });
    }

    /**
     * Recreate a projection instance from serialized metadata.
     * @param {object} metadata
     * @param {{ fileName?: string, hmac?: function(string): string, typeAccessor?: function(object): string }} [options]
     * @returns {Projection}
     */
    static fromMetadata(metadata, options = {}) {
        assert(metadata && typeof metadata === 'object', 'Invalid projection metadata.');
        if (metadata.kind === 'composite-projection') {
            return CompositeProjection.fromMetadata(metadata, options);
        }
        assert(metadata.kind === 'projection', 'Invalid projection metadata kind.');
        const hmac = options.hmac;
        const deserialize = (matcherMetadata) => {
            if (!matcherMetadata) {
                return undefined;
            }
            if (typeof matcherMetadata.matcher === 'string') {
                assert(typeof hmac === 'function', 'Must provide options.hmac to restore function projections.');
            }
            return buildMatcherFromMetadata(matcherMetadata, hmac);
        };
        const handlers = metadata.handlersKind === 'function'
            ? deserialize(metadata.handlers)
            : Object.fromEntries(
                Object.entries(metadata.handlers || {}).map(([eventType, reducerMetadata]) => [eventType, deserialize(reducerMetadata)])
            );
        const projection = new Projection(metadata.name, {
            initialState: metadata.initialState,
            matcher: deserialize(metadata.matcher),
            handlers
        }, {
            ...options,
            fileName: options.fileName || null
        });
        projection.reset();
        return projection;
    }

    /**
     * Compose multiple projections into one composite projection.
     * @param {string} name
     * @param {object<string, Projection|object>} projections
     * @param {{ matcher?: object|function(object): boolean, hmac?: function(string): string, typeAccessor?: function(object): string }} [options]
     * @returns {CompositeProjection}
     */
    static compose(name, projections, options = {}) {
        return new CompositeProjection(name, projections, options);
    }
}

class CompositeProjection extends Projection {

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
        return new CompositeProjection(metadata.name, projections, {
            ...options,
            matcher: deserializeMatcher(metadata.matcher)
        });
    }
}

export default Projection;
export { CompositeProjection };
