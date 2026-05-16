const ndjsonContentType = 'application/x-ndjson; charset=utf-8';

function writeNdjson(response, eventStream, headers = {}) {
    response.status(200);
    response.set({
        'content-type': ndjsonContentType,
        ...headers
    });

    const pump = () => {
        let next;
        while ((next = eventStream.next()) !== false) {
            if (!response.write(JSON.stringify(next) + '\n')) {
                response.once('drain', pump);
                return;
            }
        }
        response.end();
    };

    pump();
}

export { writeNdjson };
