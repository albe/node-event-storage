import { HttpError, sendJson } from '../../http/errors.js';

function registerGetVersionRoute(app, eventStore) {
    app.get(/^\/streams\/(.+)\/version$/, (request, response) => {
        const streamName = decodeURIComponent(request.params[0]);
        const version = eventStore.getStreamVersion(streamName);
        if (version === -1) {
            throw new HttpError(404, `Stream "${streamName}" does not exist.`);
        }
        sendJson(response, 200, { stream: streamName, version });
    });
}

export default registerGetVersionRoute;
