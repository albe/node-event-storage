import { HttpError, sendJson } from '../../http/errors.js';
import { parseConsumerIdentifier, parsePositiveInteger } from '../../http/routeUtils.js';

/**
 * Register the long-poll consumer endpoint.
 *
 * Waits until the named consumer has processed at least `minVersion` events, then
 * responds with the consumer's current position and state.  If the consumer does
 * not reach `minVersion` within `timeoutMs`, a 408 Request Timeout is returned.
 *
 * The consumer must be registered in the EventStore's `consumers` map (i.e. started via PUT).
 */
function registerGetConsumerUntilRoute(app, eventStore, timeoutMs = 10_000) {
    app.get('/consumers/:identifier/until/:minVersion', async (request, response) => {
        const identifier = parseConsumerIdentifier(request.params.identifier);
        const minVersion = parsePositiveInteger(request.params.minVersion, 'minVersion');

        const consumer = eventStore.getConsumer(identifier);
        if (!consumer) {
            throw new HttpError(404, `Consumer "${identifier}" is not running. Start it via PUT before polling.`);
        }

        if (consumer.position >= minVersion) {
            return sendJson(response, 200, { identifier, stream: consumer.streamName, position: consumer.position, state: consumer.state });
        }

        await new Promise((resolve, reject) => {
            const timer = setTimeout(() => {
                cleanup();
                reject(new HttpError(408, `Consumer "${identifier}" did not reach version ${minVersion} within ${timeoutMs}ms.`));
            }, timeoutMs);

            function cleanup() {
                clearTimeout(timer);
                consumer.removeListener('progress', onProgress);
                consumer.removeListener('error', onError);
            }

            function onProgress(position) {
                if (position < minVersion) {
                    return;
                }
                cleanup();
                resolve();
            }

            function onError(err) {
                cleanup();
                reject(new HttpError(500, `Consumer "${identifier}" encountered an error: ${err.message}`));
            }

            consumer.on('progress', onProgress);
            consumer.once('error', onError);
        });

        sendJson(response, 200, { identifier, stream: consumer.streamName, position: consumer.position, state: consumer.state });
    });
}

export default registerGetConsumerUntilRoute;
