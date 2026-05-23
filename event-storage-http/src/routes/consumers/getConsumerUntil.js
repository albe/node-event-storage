import { HttpError, sendJson } from '../../http/errors.js';
import { buildConsumerName, consumerNameToStream, parseConsumerIdentifier, parsePositiveInteger, scanConsumersAsync } from '../../http/routeUtils.js';

/**
 * Register the long-poll consumer endpoint.
 *
 * Waits until the named consumer has processed at least `minVersion` events, then
 * responds with the consumer's current position and state.  If the consumer does
 * not reach `minVersion` within `timeoutMs`, a 408 Request Timeout is returned.
 *
 * This endpoint requires the consumer to be running in memory (started via PUT).
 */
function registerGetConsumerUntilRoute(app, eventStore, consumerRegistry, timeoutMs = 10_000) {
    app.get('/consumers/:identifier/until/:minVersion', async (request, response) => {
        const identifier = parseConsumerIdentifier(request.params.identifier);
        const minVersion = parsePositiveInteger(request.params.minVersion, 'minVersion');

        const consumers = await scanConsumersAsync(eventStore);
        const matchesByIdentifier = consumers.filter(name => name.endsWith(`.${identifier}`));
        if (matchesByIdentifier.length === 0) {
            throw new HttpError(404, `Consumer "${identifier}" does not exist.`);
        }
        if (matchesByIdentifier.length > 1) {
            throw new HttpError(409, `Consumer identifier "${identifier}" is ambiguous.`, matchesByIdentifier);
        }

        const consumerInfo = consumerNameToStream(matchesByIdentifier[0]);
        const consumerName = buildConsumerName(consumerInfo.stream, consumerInfo.identifier);
        const runningConsumer = consumerRegistry.get(consumerName);

        // Read position from a fresh consumer object so we don't need the registry.
        const consumerForPosition = eventStore.getConsumer(consumerInfo.stream, consumerInfo.identifier);
        if (consumerForPosition.position >= minVersion) {
            return sendJson(response, 200, {
                name: consumerInfo.name,
                identifier: consumerInfo.identifier,
                stream: consumerInfo.stream,
                position: consumerForPosition.position,
                state: consumerForPosition.state
            });
        }

        if (!runningConsumer) {
            throw new HttpError(409, `Consumer "${identifier}" is not running. Start it via PUT before polling.`);
        }

        // Long-poll: wait for the consumer to advance to minVersion.
        await new Promise((resolve, reject) => {
            let settled = false;

            const timer = setTimeout(() => {
                cleanup();
                reject(new HttpError(408, `Consumer "${identifier}" did not reach version ${minVersion} within ${timeoutMs}ms.`));
            }, timeoutMs);

            function checkPosition() {
                if (settled) {
                    return;
                }
                if (runningConsumer.position >= minVersion) {
                    cleanup();
                    resolve();
                }
            }

            function onData() {
                // Position is updated synchronously AFTER push() returns, so defer the
                // check to the next iteration of the event loop to read the new value.
                setImmediate(checkPosition);
            }

            function onError(err) {
                cleanup();
                reject(new HttpError(500, `Consumer "${identifier}" encountered an error: ${err.message}`));
            }

            function cleanup() {
                if (settled) {
                    return;
                }
                settled = true;
                clearTimeout(timer);
                runningConsumer.removeListener('data', onData);
                runningConsumer.removeListener('error', onError);
            }

            runningConsumer.on('data', onData);
            runningConsumer.once('error', onError);

            // Re-check after registering the listener to avoid a race where the
            // consumer advanced between the initial check and the listener setup.
            checkPosition();
        });

        sendJson(response, 200, {
            name: consumerInfo.name,
            identifier: consumerInfo.identifier,
            stream: consumerInfo.stream,
            position: runningConsumer.position,
            state: runningConsumer.state
        });
    });
}

export default registerGetConsumerUntilRoute;
