import events from 'events';
import fs from 'fs';
import path from 'path';
import { assert } from '../utils/util.js';
import { ensureDirectory } from '../utils/fsUtil.js';

/**
 * `Consumer` — a thin convenience wrapper over a fluent {@link EventStreamQuery}
 * that adds flat-file checkpoint + state persistence and an optional pure
 * projection reducer. It is the 2.0 consumer model from
 * `concepts/2026-06-25-event-stream-api-design.md`.
 *
 * Checkpoint basis depends on the source:
 *   - a single `stream(name)` source checkpoints by **stream version** and uses
 *     the legacy on-disk path + binary format, so it is binary-compatible with —
 *     and can recover — a legacy {@link Consumer} for the same (stream, name);
 *   - `all()` checkpoints by global position but also uses the legacy `_all` path
 *     (compatible with a legacy `_all` consumer);
 *   - every other source (`category`/`join`) checkpoints by global
 *     position in a new `<name>.v2` file.
 *
 * The persisted record is always the legacy binary layout: a little-endian int32
 * position followed by the JSON-encoded state.
 *
 * @template S
 */
class Consumer extends events.EventEmitter {
    /**
     * @template S
     * @param {string} name Persistence identity (the consumer/identifier name).
     * @param {{
     *   source: import('./EventStreamQuery.js').default,
     *   apply?: (state: S|null, event: object) => S,
     *   initialState?: S
     * }} options Pure projection description.
     */
    constructor(name, options) {
        super();
        assert(typeof name === 'string' && name !== '', 'Must specify a consumer name.');
        assert(options && typeof options === 'object' && options.source, 'Must specify a source EventStreamQuery.');

        this.name = name;
        this.source = options.source;
        this.apply = options.apply ?? null;
        this.initialState = options.initialState ?? null;
        this.store = this.source._store;
        assert(this.store && this.store.storage, 'The source must be a fluent query bound to an EventStore.');

        this.configureDurability();
        this.running = false;
        this.abortRun = null;
        this.idleTimer = null;
        this.restore();
    }

    /**
     * Derive the checkpoint basis and on-disk file name from the source, choosing
     * legacy-compatible paths for single-stream and `_all` sources.
     * @private
     */
    configureDurability() {
        const source = this.source._source;
        if (source.kind === 'stream') {
            this.basis = 'stream';
            this.streamName = source.name;
            this.indexName = `stream-${source.name}`;
        } else if (source.kind === 'all') {
            this.basis = 'global';
            this.indexName = '_all';
        } else {
            this.basis = 'global';
            this.indexName = null;
        }

        const consumerDirectory = path.join(this.store.storage.indexDirectory, 'consumers');
        // Stream-relative and _all consumers use the legacy path for binary compatibility.
        // Multi-source (category / join) consumers use the 'global.' prefix.
        const fileBase = this.indexName
            ? `${this.store.storage.storageFile}.${this.indexName}.${this.name}`
            : `${this.store.storage.storageFile}.global.${this.name}`;
        this.fileName = path.join(consumerDirectory, fileBase);
        // The file may be nested when a hierarchical stream name contains '/'.
        ensureDirectory(path.dirname(this.fileName));
    }

    /**
     * Read the persisted checkpoint + state (legacy binary layout). Falls back to
     * the initial state when no file exists.
     * @private
     */
    restore() {
        try {
            const data = fs.readFileSync(this.fileName);
            this.position = data.readInt32LE(0);
            this.state = JSON.parse(data.toString('utf8', 4));
        } catch {
            this.position = 0;
            this.state = this.initialState;
        }
    }

    /**
     * Current in-memory projection state.
     * @returns {S|null}
     */
    get checkpoint() {
        return this.position;
    }

    /**
     * Persist the current checkpoint + state atomically (legacy binary layout).
     * Optionally set a new state first (value or `(prev) => next` updater).
     *
     * @param {S|((prev: S|null) => S)} [newState]
     */
    setState(newState) {
        if (newState !== undefined) {
            this.state = typeof newState === 'function' ? newState(this.state) : newState;
        }
        const stateJson = JSON.stringify(this.state ?? null);
        const buffer = Buffer.allocUnsafe(4 + Buffer.byteLength(stateJson));
        buffer.writeInt32LE(this.position, 0);
        buffer.write(stateJson, 4);
        const tmp = `${this.fileName}.${this.position}.${process.pid}.tmp`;
        fs.writeFileSync(tmp, buffer);
        fs.renameSync(tmp, this.fileName);
        this.emit('persisted', this.position, this.state);
    }

    /**
     * The current end of the addressed space — the source stream's version for a
     * stream-basis consumer, otherwise the global log length.
     * @private
     * @returns {number}
     */
    tail() {
        if (this.basis === 'stream') {
            const version = this.store.getStreamVersion(this.streamName);
            return version < 0 ? 0 : version;
        }
        return this.store.length;
    }

    /**
     * Snapshot iterator over events after the current checkpoint up to the tail,
     * in the consumer's basis.
     * @private
     * @returns {Iterator<object>}
     */
    pendingEvents() {
        if (this.basis === 'stream') {
            return this.source.streamRange().from(this.position + 1)[Symbol.iterator]();
        }
        return this.source.from(this.position + 1)[Symbol.iterator]();
    }

    /**
     * Apply one event: run the reducer (if any), advance the checkpoint in the
     * consumer's basis, and emit `progress`.
     * @private
     * @param {object} event
     */
    consumeEvent(event) {
        if (this.apply) {
            this.state = this.apply(this.state, event);
        }
        this.position = this.basis === 'stream' ? event.streamVersion : event.globalPosition;
        this.emit('progress', this.position, this.state);
    }

    /**
     * Continuously consume from the source, resuming after the checkpoint. Yields
     * each event after applying it and advancing the checkpoint; does not persist
     * (callers persist via `setState()`). Emits `caught-up` whenever the backlog
     * drains to the current tail. Ends when the signal is aborted.
     *
     * @param {{ signal?: AbortSignal }} [options]
     * @yields {object}
     * @returns {AsyncGenerator<object, void, undefined>}
     */
    async *consume({ signal } = {}) {
        let caughtUpEmitted = false;
        while (!signal?.aborted) {
            const waitNext = this.nextCommit(signal);
            let delivered = 0;
            for (const event of this.pendingEvents()) {
                this.consumeEvent(event);
                delivered++;
                yield event;
                if (signal?.aborted) {
                    return;
                }
            }
            if (delivered > 0) {
                caughtUpEmitted = false;
            }
            if (!signal?.aborted && this.position >= this.tail() && !caughtUpEmitted) {
                caughtUpEmitted = true;
                this.emit('caught-up', this.position, this.state);
            }
            await waitNext;
        }
    }

    /**
     * Run continuously, persisting per `persistEvery` (`1` = every event, `N`,
     * or `'idle'`). Resolves only when the signal is aborted (or `stop()`).
     *
     * @param {{ signal?: AbortSignal, persistEvery?: number | 'idle' }} [options]
     * @returns {Promise<void>}
     */
    async run({ signal, persistEvery = 1 } = {}) {
        assert(!this.running, 'Consumer is already running.');
        this.running = true;
        const controller = signal ? null : new AbortController();
        const effectiveSignal = signal ?? controller.signal;
        this.abortRun = () => controller?.abort();

        let sincePersist = 0;
        try {
            for await (const event of this.consume({ signal: effectiveSignal })) {
                void event;
                if (persistEvery === 'idle') {
                    this.scheduleIdlePersist();
                } else if (++sincePersist >= persistEvery) {
                    sincePersist = 0;
                    this.setState();
                }
            }
        } finally {
            this.clearIdleTimer();
            this.setState();
            this.running = false;
            this.abortRun = null;
        }
    }

    /**
     * Resolve once the checkpoint reaches `position`. Rejects with a `TIMEOUT`
     * error after `timeoutMs` (if given), or when `signal` aborts. This is the
     * primitive behind a "wait until this consumer reaches version N" endpoint.
     *
     * @param {number} position Target checkpoint (in the consumer's basis).
     * @param {{ timeoutMs?: number, signal?: AbortSignal }} [options]
     * @returns {Promise<number>} The reached checkpoint.
     */
    whenReached(position, { timeoutMs, signal } = {}) {
        return new Promise((resolve, reject) => {
            if (this.position >= position) {
                resolve(this.position);
                return;
            }
            const cleanup = () => {
                this.off('progress', onProgress);
                clearTimeout(timer);
                signal?.removeEventListener('abort', onAbort);
            };
            const onProgress = () => {
                if (this.position >= position) {
                    cleanup();
                    resolve(this.position);
                }
            };
            const onAbort = () => {
                cleanup();
                reject(Object.assign(new Error('Aborted while waiting for consumer position.'), { code: 'ABORTED' }));
            };
            const timer = timeoutMs
                ? setTimeout(() => {
                    cleanup();
                    reject(Object.assign(new Error(`Consumer did not reach position ${position} within ${timeoutMs}ms.`), { code: 'TIMEOUT' }));
                }, timeoutMs)
                : null;
            this.on('progress', onProgress);
            signal?.addEventListener('abort', onAbort, { once: true });
        });
    }

    /**
     * @private
     * @param {AbortSignal} [signal]
     * @returns {Promise<void>}
     */
    nextCommit(signal) {
        return new Promise(resolve => {
            const cleanup = () => {
                this.store.removeListener('commit', onChange);
                this.store.removeListener('stream-created', onChange);
                this.store.removeListener('stream-available', onChange);
                signal?.removeEventListener('abort', onChange);
            };
            const onChange = () => {
                cleanup();
                resolve();
            };
            this.store.on('commit', onChange);
            this.store.on('stream-created', onChange);
            this.store.on('stream-available', onChange);
            signal?.addEventListener('abort', onChange, { once: true });
        });
    }

    /**
     * @private
     */
    scheduleIdlePersist() {
        this.clearIdleTimer();
        this.idleTimer = setTimeout(() => {
            this.idleTimer = null;
            this.setState();
        }, 50);
    }

    /**
     * @private
     */
    clearIdleTimer() {
        if (this.idleTimer) {
            clearTimeout(this.idleTimer);
            this.idleTimer = null;
        }
    }

    /**
     * Stop a running `run()` loop (when started without an external signal).
     */
    stop() {
        this.abortRun?.();
    }

    /**
     * Reset to a new initial state and starting position, overwriting the
     * persisted files. `from` is the position (in the consumer's basis) to
     * (re)start from (inclusive, default 1 = beginning).
     *
     * @param {{ initialState?: S, from?: number }} [options]
     * @returns {Promise<void>}
     */
    async reset({ initialState, from = 1 } = {}) {
        const wasRunning = this.running;
        if (wasRunning) {
            this.stop();
            await new Promise(resolve => setImmediate(resolve));
        }
        this.state = initialState ?? this.initialState;
        this.position = Math.max(0, from - 1);
        this.setState();
        if (wasRunning) {
            this.run().catch(error => this.emit('error', error));
        }
    }
}

export { Consumer };
export default Consumer;
