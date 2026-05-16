import { HttpError, sendJson } from '../../http/errors.js';
import { commitAsync, parseCondition, parseExpectedVersion, parseStreamName } from '../../http/routeUtils.js';

function registerPostCommitRoute(app, eventStore) {
    app.post(/^\/streams\/(.+)\/commit$/, async (request, response) => {
        const streamName = parseStreamName(decodeURIComponent(request.params[0]));
        const body = request.body ?? {};
        if (!body || typeof body !== 'object' || Array.isArray(body)) {
            throw new HttpError(400, 'Commit payload must be a JSON object.');
        }
        if (!Array.isArray(body.events) || body.events.length === 0) {
            throw new HttpError(400, 'Commit payload must include a non-empty events array.');
        }

        const expectedVersion = body.condition !== undefined
            ? parseCondition(body.condition)
            : parseExpectedVersion(body.expectedVersion);
        const metadata = body.metadata && typeof body.metadata === 'object' && !Array.isArray(body.metadata) ? body.metadata : {};
        const commit = await commitAsync(eventStore, streamName, body.events, expectedVersion, metadata);
        sendJson(response, 201, commit);
    });
}

export default registerPostCommitRoute;
