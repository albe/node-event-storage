import { sendJson } from '../../http/errors.js';
import { consumerNameToStream, scanConsumersAsync } from '../../http/routeUtils.js';

function registerGetConsumersRoute(app, eventStore) {
    app.get('/consumers', async (request, response) => {
        const consumers = await scanConsumersAsync(eventStore);
        sendJson(response, 200, {
            consumers: consumers.map(consumerNameToStream)
        });
    });
}

export default registerGetConsumersRoute;
