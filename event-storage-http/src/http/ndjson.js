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

function writeRawNdjson(response, rawStream, headers = {}) {
    response.status(200);
    response.set({
        'content-type': ndjsonContentType,
        ...headers
    });
    rawStream.pipe(response);
}

export { writeNdjson, writeRawNdjson };
