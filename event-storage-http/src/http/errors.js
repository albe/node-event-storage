import { OptimisticConcurrencyError } from '../../../index.js';

const jsonContentType = 'application/json; charset=utf-8';

class HttpError extends Error {
    constructor(status, message, details = undefined) {
        super(message);
        this.status = status;
        this.details = details;
    }
}

function mapErrorStatus(error) {
    if (error instanceof HttpError) {
        return error.status;
    }
    if (typeof error.status === 'number') {
        return error.status;
    }
    if (error instanceof OptimisticConcurrencyError) {
        return 409;
    }
    if (/does not exist|No streams for category/.test(error.message)) {
        return 404;
    }
    if (/already exists|already closed|Can not recreate stream|read-only mode|Optimistic concurrency error/.test(error.message)) {
        return 409;
    }
    if (/Must specify|Must provide|Invalid|No events specified|Specify either/.test(error.message)) {
        return 400;
    }
    return 500;
}

function sendJson(response, status, payload, headers = {}) {
    response.status(status);
    response.set({
        'content-type': jsonContentType,
        ...headers
    });
    response.send(JSON.stringify(payload));
}

function sendError(error, request, response, next) {
    if (response.headersSent) {
        next(error);
        return;
    }

    const status = mapErrorStatus(error);
    sendJson(response, status, {
        error: error.message,
        ...(error.details ? { details: error.details } : {})
    });
}

export { HttpError, sendJson, sendError };
