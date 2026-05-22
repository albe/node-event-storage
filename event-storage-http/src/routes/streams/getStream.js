import { HttpError } from '../../http/errors.js';
import { writeNdjson } from '../../http/ndjson.js';
import { buildReadWindow, parseMatcher, splitReadStreamPath } from '../../http/routeUtils.js';

function registerGetStreamRoute(app, eventStore) {
    app.get(/^\/streams\/(?!join(?:\/|$))(?!category(?:\/|$))(.+)$/, (request, response) => {
        const { resourceName: streamName, options } = splitReadStreamPath(request.params[0]);
        const filter = parseMatcher(request.query.filter, 'filter');
        const version = eventStore.getStreamVersion(streamName);
        if (version === -1) {
            throw new HttpError(404, `Stream "${streamName}" does not exist.`);
        }
        const { from, until } = buildReadWindow(version, options);
        const stream = eventStore.getEventStream(streamName, from, until, filter, true);
        writeNdjson(response, stream, {
            'x-event-store-stream': streamName,
            'x-event-store-version': String(version)
        });
    });
}

export default registerGetStreamRoute;
