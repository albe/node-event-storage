import { HttpError } from '../../http/errors.js';
import { writeNdjson } from '../../http/ndjson.js';
import { applyMatcher, buildReadWindow, getQueryValues, parseMatcher, parseReadOptions, parseStreamName } from '../../http/routeUtils.js';

function registerGetJoinRoute(app, eventStore) {
    app.get(/^\/streams\/join(?:\/(.*))?$/, (request, response) => {
        const rawOptions = request.params[0] || '';
        const filter = parseMatcher(request.query.filter, 'filter');
        const streamNames = getQueryValues(request.query.streams).map(streamName => parseStreamName(streamName, 'streams'));
        if (streamNames.length === 0) {
            throw new HttpError(400, 'streams query parameter is required.');
        }

        const options = parseReadOptions(rawOptions);
        const { from, until } = buildReadWindow(eventStore.length, options);
        const stream = applyMatcher(
            eventStore.fromStreams(`join:${streamNames.join(',')}`, streamNames, from, until),
            filter
        );
        writeNdjson(response, stream, {
            'x-event-store-streams': streamNames.join(',')
        });
    });
}

export default registerGetJoinRoute;
