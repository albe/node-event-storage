import { sendJson } from '../../http/errors.js';

function registerGetConsumersRoute(app, eventStore) {
    app.get('/consumers', (request, response) => {
        // Return the current in-memory registry immediately.
        const consumers = [...eventStore.consumers.entries()].map(([identifier, consumer]) => ({
            identifier,
            stream: consumer.streamName
        }));

        // Fire off an async filesystem scan so consumers created externally are eventually
        // added to the registry and visible in subsequent calls.
        eventStore.scanConsumers((err) => {
            /* istanbul ignore next */
            if (err) {
                console.error('[EventStoreHttpApi] Background consumer scan error:', err);
            }
        }, true);

        sendJson(response, 200, { consumers });
    });
}

export default registerGetConsumersRoute;
