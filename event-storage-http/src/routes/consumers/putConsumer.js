import { once } from 'events';
import { HttpError, sendJson } from '../../http/errors.js';
import { buildConsumerName, scanConsumersAsync, splitConsumerStreamPath } from '../../http/routeUtils.js';

function registerPutConsumerRoute(app, eventStore) {
    app.put(/^\/consumers\/([^/]+)\/stream\/(.+)$/, async (request, response) => {
        const identifier = decodeURIComponent(request.params[0]);
        const { resourceName: stream, from } = splitConsumerStreamPath(request.params[1]);
        const initialState = request.body === undefined ? {} : request.body;
        if (!initialState || typeof initialState !== 'object' || Array.isArray(initialState)) {
            throw new HttpError(400, 'Consumer payload must be a JSON object.');
        }

        const consumerName = buildConsumerName(stream, identifier);
        const exists = (await scanConsumersAsync(eventStore)).includes(consumerName);
        const consumer = eventStore.getConsumer(stream, identifier, initialState, from);
        if (!exists) {
            const persisted = once(consumer, 'persisted');
            consumer.reset(initialState, from);
            await persisted;
        }
        sendJson(response, exists ? 200 : 201, {
            identifier,
            stream,
            position: consumer.position,
            state: consumer.state
        });
    });
}

export default registerPutConsumerRoute;
