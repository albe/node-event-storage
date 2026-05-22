import { HttpError } from '../../http/errors.js';
import { writeNdjson } from '../../http/ndjson.js';
import { getQueryValues, parseMatcher, parseRevision, parseStreamName, resolveBoundary, serializeCondition } from '../../http/routeUtils.js';

function registerGetQueryRoute(app, eventStore) {
    app.get(['/query', '/query/from/:revision'], (request, response) => {
        const types = getQueryValues(request.query.types).map(type => parseStreamName(type, 'types'));
        if (types.length === 0) {
            throw new HttpError(400, 'types query parameter is required.');
        }

        const filter = parseMatcher(request.body?.matcher ?? request.body ?? request.query.filter, 'filter');
        const parsedRevision = request.params.revision
            ? parseRevision(request.params.revision, 'from')
            : undefined;
        const minRevision = resolveBoundary(parsedRevision, 1, eventStore.length);
        const { stream, condition } = eventStore.query(types, filter, minRevision, true);
        writeNdjson(response, stream, {
            'x-event-store-query-condition': serializeCondition(condition, filter),
            'x-event-store-query-types': types.join(',')
        });
    });
}

export default registerGetQueryRoute;
