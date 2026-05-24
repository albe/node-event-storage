import { once } from 'events';
import vm from 'vm';
import { HttpError, sendJson } from '../../http/errors.js';
import { buildConsumerName, parseConsumerIdentifier, scanConsumersAsync, splitConsumerStreamPath } from '../../http/routeUtils.js';

/**
 * Compile a serialized handler function string into a callable function.
 * The handler is invoked with `(event, state)` and should return the new state,
 * or `undefined` to leave state unchanged.
 *
 * The function is compiled inside a `vm.createContext({})` sandbox so that the
 * handler body cannot access Node.js globals (`process`, `require`, `global`, etc.).
 *
 * WARNING: This uses `vm.runInNewContext()` to compile user-supplied code and must only be
 * called from a trusted client context. Do not expose this endpoint to untrusted callers.
 *
 * @param {string} handlerCode Serialized JavaScript function, e.g. `"(event, state) => ({ ...state, count: state.count + 1 })"`.
 * @returns {function} The compiled handler function.
 * @throws {HttpError} 400 if handlerCode is missing, not a string, or not a valid function.
 */
function compileHandler(handlerCode) {
    if (typeof handlerCode !== 'string' || !handlerCode.trim()) {
        throw new HttpError(400, 'Consumer payload must include a "handler" function string.');
    }
    try {
        // Run in an empty context so the handler cannot reach process, require, global, etc.
        const fn = vm.runInNewContext('(' + handlerCode + ')', Object.freeze({}));
        if (typeof fn !== 'function') {
            throw new Error('Not a function');
        }
        return fn;
    } catch {
        throw new HttpError(400, 'handler must be a valid JavaScript function.');
    }
}

function registerPutConsumerRoute(app, eventStore) {
    app.put(/^\/consumers\/([^/]+)\/stream\/(.+)$/, async (request, response) => {
        const identifier = parseConsumerIdentifier(decodeURIComponent(request.params[0]));
        const { resourceName: stream, from } = splitConsumerStreamPath(request.params[1]);

        const body = request.body;
        if (!body || typeof body !== 'object' || Array.isArray(body)) {
            throw new HttpError(400, 'Consumer payload must be a JSON object.');
        }

        const handlerFn = compileHandler(body.handler);
        const initialState = body.state ?? {};
        if (typeof initialState !== 'object' || Array.isArray(initialState) || initialState === null) {
            throw new HttpError(400, 'Consumer state must be a JSON object.');
        }

        const consumerName = buildConsumerName(stream, identifier);

        // Stop any previously registered consumer for this identifier so the
        // new handler and fresh position take effect cleanly.
        const existing = eventStore.consumers.get(identifier);
        if (existing) {
            existing.stop();
            eventStore.consumers.delete(identifier);
        }

        const exists = (await scanConsumersAsync(eventStore)).some(c => c.name === consumerName);
        const consumer = eventStore.getConsumer(stream, identifier, initialState, from);
        if (!exists) {
            const persisted = once(consumer, 'persisted');
            consumer.reset(initialState, from);
            await persisted;
        }

        // Register the compiled handler: called for every event, may update state.
        consumer.on('data', (event) => {
            try {
                const newState = handlerFn(event, consumer.state);
                if (newState !== undefined) {
                    consumer.setState(newState);
                }
            } catch (err) {
                consumer.emit('error', err);
            }
        });

        consumer.on('error', (err) => {
            console.error('[EventStoreHttpApi] Consumer "%s" error:', consumerName, err);
            consumer.stop();
            eventStore.consumers.delete(identifier);
        });

        // Keep the consumer running in memory so it stays up-to-date.
        consumer.start();

        sendJson(response, exists ? 200 : 201, {
            identifier,
            stream,
            position: consumer.position,
            state: consumer.state
        });
    });
}

export default registerPutConsumerRoute;
