import { writeNdjson } from '../../http/ndjson.js';
import { buildReadWindow, parseMatcher, splitReadStreamPath } from '../../http/routeUtils.js';

function registerGetCategoryRoute(app, eventStore) {
    app.get(/^\/streams\/category\/(.+)$/, (request, response) => {
        const { resourceName: category, options } = splitReadStreamPath(request.params[0]);
        const filter = parseMatcher(request.query.filter, 'filter');
        const categoryStream = eventStore.getEventStreamForCategory(category, 1, -1, filter, true);
        const { from, until } = buildReadWindow(categoryStream.streamIndex.length, options);
        writeNdjson(response, categoryStream.from(from).until(until), {
            'x-event-store-category': category
        });
    });
}

export default registerGetCategoryRoute;
