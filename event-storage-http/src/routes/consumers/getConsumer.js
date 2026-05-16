import { HttpError, sendJson } from '../../http/errors.js';
import { consumerNameToStream, parseConsumerIdentifier, scanConsumersAsync } from '../../http/routeUtils.js';

function registerGetConsumerRoute(app, eventStore) {
    app.get('/consumers/:identifier', async (request, response) => {
        const identifier = parseConsumerIdentifier(request.params.identifier);
        const consumers = await scanConsumersAsync(eventStore);
        const matchesByIdentifier = consumers.filter(name => name.endsWith(`.${identifier}`));
        if (matchesByIdentifier.length === 0) {
            throw new HttpError(404, `Consumer "${identifier}" does not exist.`);
        }
        if (matchesByIdentifier.length > 1) {
            throw new HttpError(409, `Consumer identifier "${identifier}" is ambiguous.`, matchesByIdentifier);
        }
        const consumerInfo = consumerNameToStream(matchesByIdentifier[0]);
        const consumer = eventStore.getConsumer(consumerInfo.stream, consumerInfo.identifier);
        sendJson(response, 200, {
            name: consumerInfo.name,
            identifier: consumerInfo.identifier,
            stream: consumerInfo.stream,
            position: consumer.position,
            state: consumer.state
        });
    });
}

export default registerGetConsumerRoute;
