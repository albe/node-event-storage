import http from 'http';
import express from 'express';
import { once } from 'events';
import { HttpError, sendError } from './http/errors.js';
import { waitForReadyMiddleware } from './http/routeUtils.js';
import registerGetConsumerRoute from './routes/consumers/getConsumer.js';
import registerGetConsumersRoute from './routes/consumers/getConsumers.js';
import registerGetConsumerUntilRoute from './routes/consumers/getConsumerUntil.js';
import registerPutConsumerRoute from './routes/consumers/putConsumer.js';
import registerGetQueryRoute from './routes/query/getQuery.js';
import registerGetCategoryRoute from './routes/streams/getCategory.js';
import registerGetJoinRoute from './routes/streams/getJoin.js';
import registerGetStreamRoute from './routes/streams/getStream.js';
import registerGetVersionRoute from './routes/streams/getVersion.js';
import registerPostCommitRoute from './routes/streams/postCommit.js';
import registerPutStreamRoute from './routes/streams/putStream.js';

class EventStoreHttpApi {
    constructor(eventStore, options = {}) {
        if (!eventStore) {
            throw new Error('eventStore is required.');
        }
        const storage = eventStore.storage;
        this.eventStore = eventStore;
        this.options = options;
        this.server = null;
        this.consumerRegistry = new Map();
        this.ready = storage?.initialized === true
            ? Promise.resolve()
            : once(eventStore, 'ready').then(() => undefined);
        this.app = this.createApp();
    }

    createApp() {
        const app = express();
        app.disable('x-powered-by');
        app.use(express.json({ limit: '1mb' }));
        app.use((request, response, next) => waitForReadyMiddleware(this.ready, request, response, next));

        registerGetConsumersRoute(app, this.eventStore);
        registerGetConsumerRoute(app, this.eventStore);
        registerGetConsumerUntilRoute(app, this.eventStore, this.consumerRegistry, this.options.consumerPollTimeoutMs ?? 10_000);
        registerPutConsumerRoute(app, this.eventStore, this.consumerRegistry);
        registerGetQueryRoute(app, this.eventStore);
        registerGetJoinRoute(app, this.eventStore);
        registerGetCategoryRoute(app, this.eventStore);
        registerPostCommitRoute(app, this.eventStore);
        registerGetVersionRoute(app, this.eventStore);
        registerGetStreamRoute(app, this.eventStore);
        registerPutStreamRoute(app, this.eventStore);

        app.use((request, response, next) => {
            next(new HttpError(404, 'Unknown route.'));
        });
        app.use(sendError);
        return app;
    }

    createServer() {
        if (!this.server) {
            this.server = http.createServer(this.app);
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
}

function createEventStoreHttpServer(eventStore, options = {}) {
    return new EventStoreHttpApi(eventStore, options).createServer();
}

export default EventStoreHttpApi;
export { createEventStoreHttpServer };
