import { HttpError, sendJson } from '../../http/errors.js';
import { parseStreamName } from '../../http/routeUtils.js';

function registerGetVersionRoute(app, eventStore) {
    app.get(/^\/streams\/(.+)\/version$/, (request, response) => {
        const streamName = parseStreamName(decodeURIComponent(request.params[0]));
        const version = eventStore.getStreamVersion(streamName);
        if (version === -1) {
            throw new HttpError(404, `Stream "${streamName}" does not exist.`);
        }
        sendJson(response, 200, { stream: streamName, version });
    });
}

export default registerGetVersionRoute;
