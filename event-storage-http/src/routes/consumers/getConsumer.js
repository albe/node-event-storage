import { HttpError, sendJson } from '../../http/errors.js';
import { parseConsumerIdentifier } from '../../http/routeUtils.js';

function registerGetConsumerRoute(app, eventStore) {
    app.get('/consumers/:identifier', (request, response) => {
        const identifier = parseConsumerIdentifier(request.params.identifier);
        const consumer = eventStore.getConsumer(identifier);
        if (!consumer) {
            throw new HttpError(404, `Consumer "${identifier}" does not exist.`);
        }
        return sendJson(response, 200, {
            identifier,
            stream: consumer.streamName,
            position: consumer.position,
            state: consumer.state
        });
    });
}

export default registerGetConsumerRoute;
