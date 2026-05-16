import { CommitCondition, ExpectedVersion } from '../../../index.js';
import { matches } from '../../../src/metadataUtil.js';
import { HttpError } from './errors.js';

const readOptionNames = new Set(['from', 'until', 'forwards', 'backwards']);
const validStreamNamePattern = /^[A-Za-z0-9/_-]+$/;
const validConsumerIdentifierPattern = /^[A-Za-z0-9_-]+$/;

function parseJson(raw, what) {
    try {
        return JSON.parse(raw);
    } catch {
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

function createPayloadMetadataPredicate(matcher) {
    if (!matcher) {
        return null;
    }
    return (payload, metadata) => matches({ payload, metadata }, matcher);
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
        createPayloadMetadataPredicate(matcher),
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

function parseStreamName(value, source = 'stream') {
    if (typeof value !== 'string' || value === '') {
        throw new HttpError(400, `${source} must not be empty.`);
    }
    if (!validStreamNamePattern.test(value) || value.split('/').some(segment => segment === '')) {
        throw new HttpError(400, `${source} may only contain letters, numbers, "-", "_" and "/".`);
    }
    return value;
}

function parseConsumerIdentifier(value, source = 'identifier') {
    if (typeof value !== 'string' || value === '') {
        throw new HttpError(400, `${source} must not be empty.`);
    }
    if (!validConsumerIdentifierPattern.test(value)) {
        throw new HttpError(400, `${source} may only contain letters, numbers, "-" and "_".`);
    }
    return value;
}

function parseSegmentOptions(segments, startIndex = 0) {
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

function splitPathSegments(rawPath = '') {
    return rawPath
        .split('/')
        .filter(Boolean)
        .map(segment => decodeURIComponent(segment));
}

function parseReadOptions(rawPath = '') {
    return parseSegmentOptions(splitPathSegments(rawPath));
}

function splitReadStreamPath(rawPath) {
    const segments = splitPathSegments(rawPath);
    let optionStart = segments.length;
    while (optionStart >= 2 && readOptionNames.has(segments[optionStart - 2])) {
        optionStart -= 2;
    }
    const resourceName = segments.slice(0, optionStart).join('/');
    if (!resourceName) {
        throw new HttpError(404, 'Unknown route.');
    }
    return {
        resourceName: parseStreamName(resourceName),
        options: parseSegmentOptions(segments.slice(optionStart))
    };
}

function splitConsumerStreamPath(rawPath) {
    const segments = splitPathSegments(rawPath);
    let from = 0;
    if (segments.length >= 2 && segments[segments.length - 2] === 'from') {
        from = parsePositiveInteger(segments[segments.length - 1], 'from');
        segments.splice(-2, 2);
    }
    const resourceName = parseStreamName(segments.join('/'));
    if (!resourceName) {
        throw new HttpError(404, 'Unknown route.');
    }
    return { resourceName, from };
}

function applyMatcher(eventStream, matcher) {
    const predicate = createPayloadMetadataPredicate(matcher);
    return predicate ? eventStream.filter(predicate) : eventStream;
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

function getQueryValues(value) {
    if (Array.isArray(value)) {
        return value.flatMap(item => String(item).split(',')).map(item => item.trim()).filter(Boolean);
    }
    if (value === undefined) {
        return [];
    }
    return String(value).split(',').map(item => item.trim()).filter(Boolean);
}

async function waitForReadyMiddleware(promise, request, response, next) {
    try {
        await promise;
        next();
    } catch (error) {
        next(error);
    }
}

function buildConsumerName(stream, identifier) {
    return stream === '_all'
        ? `_all.${identifier}`
        : `stream-${stream}.${identifier}`;
}

export {
    applyMatcher,
    buildReadWindow,
    buildConsumerName,
    commitAsync,
    consumerNameToStream,
    createPayloadMetadataPredicate,
    getQueryValues,
    parseCondition,
    parseExpectedVersion,
    parseMatcher,
    parseReadOptions,
    parseConsumerIdentifier,
    parseRevision,
    parseSegmentOptions,
    parseStreamName,
    resolveBoundary,
    scanConsumersAsync,
    serializeCondition,
    splitConsumerStreamPath,
    splitReadStreamPath,
    waitForReadyMiddleware
};
