import { writeNdjson } from '../../http/ndjson.js';
import { applyMatcher, buildReadWindow, parseMatcher, splitReadStreamPath } from '../../http/routeUtils.js';

function registerGetCategoryRoute(app, eventStore) {
    app.get(/^\/streams\/category\/(.+)$/, (request, response) => {
        const { resourceName: category, options } = splitReadStreamPath(request.params[0]);
        const filter = parseMatcher(request.query.filter, 'filter');
        const categoryStream = applyMatcher(eventStore.getEventStreamForCategory(category), filter);
        const { from, until } = buildReadWindow(categoryStream.streamIndex.length, options);
        categoryStream.from(from).until(until);
        writeNdjson(response, categoryStream, {
            'x-event-store-category': category
        });
    });
}

export default registerGetCategoryRoute;
