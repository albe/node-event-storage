import { HttpError, sendJson } from '../../http/errors.js';
import { parseMatcher, parseStreamName } from '../../http/routeUtils.js';

function registerPutStreamRoute(app, eventStore) {
    app.put(/^\/streams\/(.+)$/, (request, response) => {
        const streamName = parseStreamName(decodeURIComponent(request.params[0]));
        const matcher = parseMatcher(request.body?.matcher ?? request.body, 'matcher');
        if (!matcher) {
            throw new HttpError(400, 'Stream creation requires a matcher object.');
        }
        const stream = eventStore.createEventStream(streamName, matcher);
        sendJson(response, 201, {
            stream: streamName,
            version: stream.version
        });
    });
}

export default registerPutStreamRoute;
