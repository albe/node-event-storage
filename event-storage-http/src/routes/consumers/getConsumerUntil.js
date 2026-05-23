import { HttpError, sendJson } from '../../http/errors.js';
import { parseConsumerIdentifier, parsePositiveInteger } from '../../http/routeUtils.js';

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

        const entry = consumerRegistry.get(identifier);
        if (!entry) {
            throw new HttpError(404, `Consumer "${identifier}" is not running. Start it via PUT before polling.`);
        }

        const { consumer, name, stream } = entry;

        if (consumer.position >= minVersion) {
            return sendJson(response, 200, { name, identifier, stream, position: consumer.position, state: consumer.state });
        }

        await new Promise((resolve, reject) => {
            let settled = false;

            const timer = setTimeout(() => {
                cleanup();
                reject(new HttpError(408, `Consumer "${identifier}" did not reach version ${minVersion} within ${timeoutMs}ms.`));
            }, timeoutMs);

            function onProgress(position) {
                if (settled || position < minVersion) {
                    return;
                }
                cleanup();
                resolve();
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
                consumer.removeListener('progress', onProgress);
                consumer.removeListener('error', onError);
            }

            consumer.on('progress', onProgress);
            consumer.once('error', onError);

            // Re-check after registering the listener to avoid a race where the
            // consumer advanced between the initial check and the listener setup.
            if (consumer.position >= minVersion) {
                cleanup();
                resolve();
            }
        });

        sendJson(response, 200, { name, identifier, stream, position: consumer.position, state: consumer.state });
    });
}

export default registerGetConsumerUntilRoute;
