import { writeNdjson } from '../../http/ndjson.js';
import { applyMatcher, buildReadWindow, parseMatcher, parseStreamName, splitReadStreamPath } from '../../http/routeUtils.js';

function registerGetCategoryRoute(app, eventStore) {
    app.get(/^\/streams\/category\/(.+)$/, (request, response) => {
        const { resourceName, options } = splitReadStreamPath(request.params[0]);
        const category = parseStreamName(resourceName, 'category');
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
