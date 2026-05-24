import { sendJson } from '../../http/errors.js';

const SCAN_DEBOUNCE_MS = 5_000;

function registerGetConsumersRoute(app, eventStore) {
    let lastScanAt = 0;

    app.get('/consumers', (request, response) => {
        // Return the current in-memory registry immediately.
        const consumers = [...eventStore.consumers.entries()].map(([identifier, consumer]) => ({
            identifier,
            stream: consumer.streamName
        }));

        // Fire off an async filesystem scan so consumers created externally are eventually
        // added to the registry and visible in subsequent calls, debounced to avoid hammering the fs.
        const now = Date.now();
        if (now - lastScanAt > SCAN_DEBOUNCE_MS) {
            lastScanAt = now;
            eventStore.scanConsumers((err) => {
                /* istanbul ignore next */
                if (err) {
                    console.error('[EventStoreHttpApi] Background consumer scan error:', err);
                }
            }, true);
        }

        sendJson(response, 200, { consumers });
    });
}

export default registerGetConsumersRoute;
