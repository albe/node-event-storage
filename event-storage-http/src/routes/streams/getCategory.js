import { writeNdjson } from '../../http/ndjson.js';
import { applyMatcher, buildReadWindow, parseMatcher, splitReadStreamPath } from '../../http/routeUtils.js';

function registerGetCategoryRoute(app, eventStore) {
    app.get(/^\/streams\/category\/(.+)$/, (request, response) => {
        const { resourceName: category, options } = splitReadStreamPath(request.params[0]);
        const filter = parseMatcher(request.query.filter, 'filter');
        const categoryStream = eventStore.getEventStreamForCategory(category);
        const { from, until } = buildReadWindow(categoryStream.streamIndex.length, options);
        const filteredStream = applyMatcher(categoryStream.from(from).until(until), filter);
        writeNdjson(response, filteredStream, {
            'x-event-store-category': category
        });
    });
}

export default registerGetCategoryRoute;
