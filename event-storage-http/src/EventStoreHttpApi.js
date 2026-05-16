import fs from 'fs';
import http from 'http';
import { once } from 'events';
import { CommitCondition, ExpectedVersion, OptimisticConcurrencyError } from '../../index.js';
import { matches } from '../../src/metadataUtil.js';

const jsonContentType = 'application/json; charset=utf-8';
const ndjsonContentType = 'application/x-ndjson; charset=utf-8';
const bodySizeLimit = 1024 * 1024;

class HttpError extends Error {
    constructor(status, message, details = undefined) {
        super(message);
        this.status = status;
        this.details = details;
    }
}

function parseJson(raw, what) {
    try {
        return JSON.parse(raw);
    } catch (error) {
        throw new HttpError(400, `Invalid ${what}.`);
    }
}

function parseMatcher(value, source) {
    if (value === undefined || value === null || value === '') {
        return null;
    }
    const matcher = typeof value === 'string' ? parseJson(value, source) : value;
    if (!matcher || typeof matcher !== 'object' || Array.isArray(matcher)) {
        throw new HttpError(400, `${source} must be a JSON object.`);
    }
    return matcher;
}

function parseExpectedVersion(value) {
    if (value === undefined || value === null) {
        return ExpectedVersion.Any;
    }
    if (typeof value === 'number') {
        return value;
    }
    if (typeof value === 'string') {
        const normalized = value.trim().toLowerCase();
        if (normalized === 'any') {
            return ExpectedVersion.Any;
        }
        if (normalized === 'emptystream' || normalized === 'empty') {
            return ExpectedVersion.EmptyStream;
        }
        if (/^-?\d+$/.test(normalized)) {
            return Number(normalized);
        }
    }
    throw new HttpError(400, 'expectedVersion must be a number, "any", or "empty".');
}

function parseCondition(value) {
    if (value === undefined || value === null) {
        return null;
    }
    const condition = typeof value === 'string' ? parseJson(value, 'condition') : value;
    if (!condition || typeof condition !== 'object' || Array.isArray(condition)) {
        throw new HttpError(400, 'condition must be a JSON object.');
    }
    if (!Array.isArray(condition.types) || condition.types.length === 0 || !condition.types.every(type => typeof type === 'string' && type !== '')) {
        throw new HttpError(400, 'condition.types must be a non-empty string array.');
    }
    if (!Number.isInteger(condition.noneMatchAfter) || condition.noneMatchAfter < 0) {
        throw new HttpError(400, 'condition.noneMatchAfter must be a non-negative integer.');
    }
    const matcher = parseMatcher(condition.matcher, 'condition.matcher');
    return new CommitCondition(
        condition.types,
        matcher ? (payload, metadata) => matches({ payload, metadata }, matcher) : null,
        condition.noneMatchAfter
    );
}

function serializeCondition(condition, matcher = null) {
    return JSON.stringify({
        types: condition.types,
        noneMatchAfter: condition.noneMatchAfter,
        ...(matcher ? { matcher } : {})
    });
}

function parseRevision(value, name) {
    if (value === undefined) {
        return undefined;
    }
    if (value === 'start') {
        return 'start';
    }
    if (value === 'end') {
        return 'end';
    }
    if (/^-?\d+$/.test(value)) {
        const parsed = Number(value);
        if (parsed === 0) {
            throw new HttpError(400, `${name} must not be 0.`);
        }
        return parsed;
    }
    throw new HttpError(400, `${name} must be an integer, "start", or "end".`);
}

function parsePositiveInteger(value, name) {
    const parsed = Number(value);
    if (!Number.isInteger(parsed) || parsed <= 0) {
        throw new HttpError(400, `${name} must be a positive integer.`);
    }
    return parsed;
}

function applyNamedSegments(segments, startIndex) {
    const options = {};
    for (let index = startIndex; index < segments.length; index += 2) {
        const name = segments[index];
        const value = segments[index + 1];
        if (value === undefined) {
            throw new HttpError(404, 'Unknown route.');
        }
        switch (name) {
        case 'from':
            options.from = parseRevision(value, 'from');
            break;
        case 'until':
            options.until = parseRevision(value, 'until');
            break;
        case 'forwards':
            if (options.direction) {
                throw new HttpError(400, 'Specify either forwards or backwards, not both.');
            }
            options.direction = 'forwards';
            options.amount = parsePositiveInteger(value, 'forwards');
            break;
        case 'backwards':
            if (options.direction) {
                throw new HttpError(400, 'Specify either forwards or backwards, not both.');
            }
            options.direction = 'backwards';
            options.amount = parsePositiveInteger(value, 'backwards');
            break;
        default:
            throw new HttpError(404, 'Unknown route.');
        }
    }
    return options;
}

function resolveBoundary(boundary, fallback, length) {
    if (boundary === undefined) {
        return fallback;
    }
    if (boundary === 'start') {
        return 1;
    }
    if (boundary === 'end') {
        return length;
    }
    return boundary;
}

function buildReadWindow(length, options = {}) {
    const lower = resolveBoundary(options.from, 1, length);
    const upper = resolveBoundary(options.until, length, length);
    if (!Number.isInteger(lower) || !Number.isInteger(upper)) {
        throw new HttpError(400, 'Invalid stream boundaries.');
    }

    const direction = options.direction || 'forwards';
    if (direction === 'forwards') {
        const from = lower;
        const until = options.amount ? Math.min(upper, from + options.amount - 1) : upper;
        return { from, until };
    }

    const from = upper;
    const until = options.amount ? Math.max(lower, upper - options.amount + 1) : lower;
    return { from, until };
}

function createDocumentPredicate(matcher) {
    if (!matcher) {
        return null;
    }
    return (document) => matches(document, matcher);
}

function createPayloadMetadataPredicate(matcher) {
    if (!matcher) {
        return null;
    }
    return (payload, metadata) => matches({ payload, metadata }, matcher);
}

function consumerNameToStream(consumerName) {
    const splitIndex = consumerName.lastIndexOf('.');
    if (splitIndex < 0) {
        throw new HttpError(500, `Invalid consumer name "${consumerName}".`);
    }
    const indexName = consumerName.slice(0, splitIndex);
    const identifier = consumerName.slice(splitIndex + 1);
    if (indexName === '_all') {
        return { identifier, stream: '_all', name: consumerName };
    }
    if (!indexName.startsWith('stream-')) {
        throw new HttpError(500, `Unsupported consumer stream "${indexName}".`);
    }
    return { identifier, stream: indexName.slice(7), name: consumerName };
}

function mapErrorStatus(error) {
    if (error instanceof HttpError) {
        return error.status;
    }
    if (error instanceof OptimisticConcurrencyError) {
        return 409;
    }
    if (/does not exist|No streams for category/.test(error.message)) {
        return 404;
    }
    if (/already exists|already closed|Can not recreate stream|read-only mode|Optimistic Concurrency error/.test(error.message)) {
        return 409;
    }
    if (/Must specify|Must provide|Invalid|No events specified|Specify either/.test(error.message)) {
        return 400;
    }
    return 500;
}

function sendJson(response, status, payload, headers = {}) {
    response.writeHead(status, {
        'content-type': jsonContentType,
        ...headers
    });
    response.end(JSON.stringify(payload));
}

function sendError(response, error) {
    const status = mapErrorStatus(error);
    sendJson(response, status, {
        error: error.message,
        ...(error.details ? { details: error.details } : {})
    });
}

async function readJsonBody(request) {
    const chunks = [];
    let size = 0;
    for await (const chunk of request) {
        size += chunk.length;
        if (size > bodySizeLimit) {
            throw new HttpError(413, `Request body exceeds ${bodySizeLimit} bytes.`);
        }
        chunks.push(chunk);
    }
    if (chunks.length === 0) {
        return undefined;
    }
    return parseJson(Buffer.concat(chunks).toString('utf8'), 'JSON body');
}

function writeNdjson(response, eventStream, matcher = null, headers = {}) {
    const predicate = createDocumentPredicate(matcher);
    response.writeHead(200, {
        'content-type': ndjsonContentType,
        ...headers
    });

    const pump = () => {
        let next;
        while ((next = eventStream.next()) !== false) {
            if (predicate && !predicate(next)) {
                continue;
            }
            if (!response.write(JSON.stringify(next) + '\n')) {
                response.once('drain', pump);
                return;
            }
        }
        response.end();
    };

    pump();
}

function commitAsync(eventStore, streamName, events, expectedVersion, metadata) {
    return new Promise((resolve, reject) => {
        try {
            eventStore.commit(streamName, events, expectedVersion, metadata, resolve);
        } catch (error) {
            reject(error);
        }
    });
}

function scanConsumersAsync(eventStore) {
    return new Promise((resolve, reject) => {
        eventStore.scanConsumers((error, consumers) => error ? reject(error) : resolve(consumers));
    });
}

class EventStoreHttpApi {
    constructor(eventStore, options = {}) {
        if (!eventStore) {
            throw new Error('eventStore is required.');
        }
        this.eventStore = eventStore;
        this.options = options;
        this.server = null;
        this.ready = eventStore.storage?.initialized === true ? Promise.resolve() : once(eventStore, 'ready').then(() => undefined);
    }

    createServer() {
        if (!this.server) {
            this.server = http.createServer(this.handleRequest.bind(this));
        }
        return this.server;
    }

    listen(...args) {
        return this.createServer().listen(...args);
    }

    close(callback) {
        if (!this.server) {
            callback?.();
            return undefined;
        }
        return this.server.close(callback);
    }

    async handleRequest(request, response) {
        try {
            await this.ready;
            const url = new URL(request.url, 'http://127.0.0.1');
            const segments = url.pathname.split('/').filter(Boolean).map(segment => decodeURIComponent(segment));
            const body = await readJsonBody(request);

            if (segments.length === 0) {
                throw new HttpError(404, 'Unknown route.');
            }

            if (request.method === 'GET' && segments.length === 1 && segments[0] === 'consumers') {
                return this.handleGetConsumers(response);
            }

            if (request.method === 'GET' && segments.length === 2 && segments[0] === 'consumers') {
                return this.handleGetConsumer(response, segments[1]);
            }

            if (request.method === 'PUT' && segments.length >= 4 && segments[0] === 'consumers' && segments[2] === 'stream') {
                return this.handlePutConsumer(response, segments, body);
            }

            if (request.method === 'GET' && segments[0] === 'query') {
                return this.handleQuery(response, segments, url, body);
            }

            if (segments[0] !== 'streams') {
                throw new HttpError(404, 'Unknown route.');
            }

            if (request.method === 'GET' && segments[1] === 'join') {
                return this.handleJoinStream(response, segments, url);
            }

            if (request.method === 'GET' && segments[1] === 'category' && segments[2]) {
                return this.handleCategoryStream(response, segments, url);
            }

            if (!segments[1]) {
                throw new HttpError(404, 'Unknown route.');
            }

            if (request.method === 'POST' && segments.length === 3 && segments[2] === 'commit') {
                return this.handleCommit(response, segments[1], body);
            }

            if (request.method === 'PUT' && segments.length === 2) {
                return this.handleCreateStream(response, segments[1], body);
            }

            if (request.method === 'GET' && segments.length === 3 && segments[2] === 'version') {
                return this.handleVersion(response, segments[1]);
            }

            if (request.method === 'GET') {
                return this.handleStream(response, segments, url);
            }

            throw new HttpError(404, 'Unknown route.');
        } catch (error) {
            sendError(response, error);
        }
    }

    async handleCommit(response, streamName, body = {}) {
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
        const commit = await commitAsync(this.eventStore, streamName, body.events, expectedVersion, metadata);
        sendJson(response, 201, commit);
    }

    handleCreateStream(response, streamName, body) {
        const matcher = parseMatcher(body?.matcher ?? body, 'matcher');
        if (!matcher) {
            throw new HttpError(400, 'Stream creation requires a matcher object.');
        }
        const stream = this.eventStore.createEventStream(streamName, matcher);
        sendJson(response, 201, {
            stream: streamName,
            version: stream.version
        });
    }

    handleVersion(response, streamName) {
        const version = this.eventStore.getStreamVersion(streamName);
        if (version === -1) {
            throw new HttpError(404, `Stream "${streamName}" does not exist.`);
        }
        sendJson(response, 200, { stream: streamName, version });
    }

    handleStream(response, segments, url) {
        const streamName = segments[1];
        const options = applyNamedSegments(segments, 2);
        const filter = parseMatcher(url.searchParams.get('filter'), 'filter');
        const version = this.eventStore.getStreamVersion(streamName);
        if (version === -1) {
            throw new HttpError(404, `Stream "${streamName}" does not exist.`);
        }
        const { from, until } = buildReadWindow(version, options);
        const stream = this.eventStore.getEventStream(streamName, from, until);
        writeNdjson(response, stream, filter, {
            'x-event-store-stream': streamName,
            'x-event-store-version': String(version)
        });
    }

    handleJoinStream(response, segments, url) {
        const options = applyNamedSegments(segments, 2);
        const filter = parseMatcher(url.searchParams.get('filter'), 'filter');
        const streamNames = [];
        for (const value of url.searchParams.getAll('streams')) {
            streamNames.push(...value.split(',').map(streamName => streamName.trim()).filter(Boolean));
        }
        if (streamNames.length === 0) {
            throw new HttpError(400, 'streams query parameter is required.');
        }
        const { from, until } = buildReadWindow(this.eventStore.length, options);
        const stream = this.eventStore.fromStreams(`join:${streamNames.join(',')}`, streamNames, from, until);
        writeNdjson(response, stream, filter, {
            'x-event-store-streams': streamNames.join(',')
        });
    }

    handleCategoryStream(response, segments, url) {
        const category = segments[2];
        const options = applyNamedSegments(segments, 3);
        const filter = parseMatcher(url.searchParams.get('filter'), 'filter');
        const categoryStream = this.eventStore.getEventStreamForCategory(category);
        const { from, until } = buildReadWindow(categoryStream.streamIndex.length, options);
        const stream = this.eventStore.getEventStreamForCategory(category, from, until);
        writeNdjson(response, stream, filter, {
            'x-event-store-category': category
        });
    }

    handleQuery(response, segments, url, body) {
        const options = applyNamedSegments(segments, 1);
        const minRevision = resolveBoundary(options.from, 1, this.eventStore.length);
        const types = [];
        for (const value of url.searchParams.getAll('types')) {
            types.push(...value.split(',').map(type => type.trim()).filter(Boolean));
        }
        if (types.length === 0) {
            throw new HttpError(400, 'types query parameter is required.');
        }
        const filter = parseMatcher(body?.matcher ?? body ?? url.searchParams.get('filter'), 'filter');
        const { stream, condition } = this.eventStore.query(types, createPayloadMetadataPredicate(filter), minRevision);
        writeNdjson(response, stream, null, {
            'x-event-store-query-condition': serializeCondition(condition, filter),
            'x-event-store-query-types': types.join(',')
        });
    }

    async handlePutConsumer(response, segments, body) {
        if (segments.length !== 4 && segments.length !== 6) {
            throw new HttpError(404, 'Unknown route.');
        }
        if (segments.length === 6 && segments[4] !== 'from') {
            throw new HttpError(404, 'Unknown route.');
        }
        const identifier = segments[1];
        const stream = segments[3];
        const from = segments.length === 6 ? parsePositiveInteger(segments[5], 'from') : 0;
        const initialState = body === undefined ? {} : body;
        if (!initialState || typeof initialState !== 'object' || Array.isArray(initialState)) {
            throw new HttpError(400, 'Consumer payload must be a JSON object.');
        }

        const consumer = this.eventStore.getConsumer(stream, identifier, initialState, from);
        const exists = fs.existsSync(consumer.fileName);
        if (!exists) {
            consumer.reset(initialState, from);
        }
        sendJson(response, exists ? 200 : 201, {
            identifier,
            stream,
            position: consumer.position,
            state: consumer.state
        });
    }

    async handleGetConsumer(response, identifier) {
        const consumers = await scanConsumersAsync(this.eventStore);
        const matchesByIdentifier = consumers.filter(name => name.endsWith(`.${identifier}`));
        if (matchesByIdentifier.length === 0) {
            throw new HttpError(404, `Consumer "${identifier}" does not exist.`);
        }
        if (matchesByIdentifier.length > 1) {
            throw new HttpError(409, `Consumer identifier "${identifier}" is ambiguous.`, matchesByIdentifier);
        }
        const consumerInfo = consumerNameToStream(matchesByIdentifier[0]);
        const consumer = this.eventStore.getConsumer(consumerInfo.stream, consumerInfo.identifier);
        sendJson(response, 200, {
            name: consumerInfo.name,
            identifier: consumerInfo.identifier,
            stream: consumerInfo.stream,
            position: consumer.position,
            state: consumer.state
        });
    }

    async handleGetConsumers(response) {
        const consumers = await scanConsumersAsync(this.eventStore);
        sendJson(response, 200, {
            consumers: consumers.map(consumerNameToStream)
        });
    }
}

function createEventStoreHttpServer(eventStore, options = {}) {
    return new EventStoreHttpApi(eventStore, options).createServer();
}

export default EventStoreHttpApi;
export { createEventStoreHttpServer };
