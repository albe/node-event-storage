import fs from 'fs/promises';
import os from 'os';
import path from 'path';
import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'events';
import EventStore from '../../index.js';
import EventStoreHttpApi from '../src/EventStoreHttpApi.js';

function commitAsync(eventStore, streamName, events) {
    return new Promise((resolve, reject) => {
        try {
            eventStore.commit(streamName, events, resolve);
        } catch (error) {
            reject(error);
        }
    });
}

async function createFixture() {
    const storageDirectory = await fs.mkdtemp(path.join(os.tmpdir(), 'event-storage-http-test-'));
    const eventStore = new EventStore({
        storageDirectory,
        typeAccessor: 'type'
    });
    await once(eventStore, 'ready');

    const api = new EventStoreHttpApi(eventStore);
    const server = api.createServer();
    await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
    const address = server.address();

    return {
        storageDirectory,
        eventStore,
        server,
        baseUrl: `http://127.0.0.1:${address.port}`
    };
}

async function destroyFixture(fixture) {
    await new Promise(resolve => fixture.server.close(resolve));
    fixture.eventStore.close();
    await fs.rm(fixture.storageDirectory, { recursive: true, force: true });
}

async function parseNdjson(response) {
    const text = await response.text();
    return text
        .trim()
        .split('\n')
        .filter(Boolean)
        .map(line => JSON.parse(line));
}

test('POST /streams/:stream/commit stores events and GET /streams/:stream/version reports the version', async () => {
    const fixture = await createFixture();
    try {
        const commitResponse = await fetch(`${fixture.baseUrl}/streams/orders-1/commit`, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({
                events: [
                    { type: 'OrderPlaced', orderId: '1' },
                    { type: 'OrderConfirmed', orderId: '1' }
                ],
                metadata: { requestId: 'req-1' }
            })
        });
        assert.equal(commitResponse.status, 201);
        const commit = await commitResponse.json();
        assert.equal(commit.streamName, 'orders-1');
        assert.equal(commit.events.length, 2);

        const versionResponse = await fetch(`${fixture.baseUrl}/streams/orders-1/version`);
        assert.equal(versionResponse.status, 200);
        assert.deepEqual(await versionResponse.json(), {
            stream: 'orders-1',
            version: 2
        });
    } finally {
        await destroyFixture(fixture);
    }
});

test('HTTP API validates stream names and consumer identifiers', async () => {
    const fixture = await createFixture();
    try {
        const validCommitResponse = await fetch(`${fixture.baseUrl}/streams/orders.v1/eu-1/commit`, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({
                events: [{ type: 'OrderPlaced', orderId: 'safe-1' }]
            })
        });
        assert.equal(validCommitResponse.status, 201);

        const validVersionResponse = await fetch(`${fixture.baseUrl}/streams/orders.v1/eu-1/version`);
        assert.equal(validVersionResponse.status, 200);

        const dottedTypeQueryResponse = await fetch(`${fixture.baseUrl}/query?types=Order.Placed`);
        assert.equal(dottedTypeQueryResponse.status, 200);

        const invalidStreamResponse = await fetch(`${fixture.baseUrl}/streams/orders..1/commit`, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({
                events: [{ type: 'OrderPlaced', orderId: 'unsafe-1' }]
            })
        });
        assert.equal(invalidStreamResponse.status, 400);
        assert.deepEqual(await invalidStreamResponse.json(), {
            error: 'stream must use segments that start with a letter or number and may contain letters, numbers, "-", "_", ".", and "/".'
        });

        const invalidJoinResponse = await fetch(`${fixture.baseUrl}/streams/join?streams=orders..1`);
        assert.equal(invalidJoinResponse.status, 400);

        const invalidQueryResponse = await fetch(`${fixture.baseUrl}/query?types=Order..Placed`);
        assert.equal(invalidQueryResponse.status, 400);

        const invalidConsumerResponse = await fetch(`${fixture.baseUrl}/consumers/reader%2F1/stream/orders.v1/eu-1`, {
            method: 'PUT',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ lastSeen: null })
        });
        assert.equal(invalidConsumerResponse.status, 400);
        assert.deepEqual(await invalidConsumerResponse.json(), {
            error: 'identifier may only contain letters, numbers, "-" and "_".'
        });
    } finally {
        await destroyFixture(fixture);
    }
});

test('PUT /streams/:stream creates matcher streams and GET /streams/:stream returns filtered NDJSON', async () => {
    const fixture = await createFixture();
    try {
        await commitAsync(fixture.eventStore, 'users-1', [
            { type: 'UserCreated', userId: '1' },
            { type: 'UserEmailUpdated', userId: '1' }
        ]);

        const createResponse = await fetch(`${fixture.baseUrl}/streams/users`, {
            method: 'PUT',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({
                stream: ['users-1']
            })
        });
        assert.equal(createResponse.status, 201);

        const matcher = encodeURIComponent(JSON.stringify({ payload: { type: 'UserEmailUpdated' } }));
        const response = await fetch(`${fixture.baseUrl}/streams/users/backwards/2?filter=${matcher}`);
        assert.equal(response.status, 200);
        assert.equal(response.headers.get('content-type'), 'application/x-ndjson; charset=utf-8');

        const events = await parseNdjson(response);
        assert.equal(events.length, 1);
        assert.equal(events[0].payload.type, 'UserEmailUpdated');
    } finally {
        await destroyFixture(fixture);
    }
});

test('GET /streams/join and /streams/category return joined NDJSON output, including nested categories', async () => {
    const fixture = await createFixture();
    try {
        await commitAsync(fixture.eventStore, 'orders-1', [{ type: 'OrderPlaced', orderId: '1' }]);
        await commitAsync(fixture.eventStore, 'orders-2', [{ type: 'OrderPlaced', orderId: '2' }]);
        await commitAsync(fixture.eventStore, 'orders/eu/1', [{ type: 'OrderPlaced', orderId: '3' }]);
        await commitAsync(fixture.eventStore, 'orders/eu/2', [{ type: 'OrderPlaced', orderId: '4' }]);

        const joinResponse = await fetch(`${fixture.baseUrl}/streams/join?streams=orders-1,orders-2`);
        assert.equal(joinResponse.status, 200);
        const joined = await parseNdjson(joinResponse);
        assert.deepEqual(joined.map(event => event.stream), ['orders-1', 'orders-2']);

        const categoryResponse = await fetch(`${fixture.baseUrl}/streams/category/orders`);
        assert.equal(categoryResponse.status, 200);
        const categoryEvents = await parseNdjson(categoryResponse);
        assert.equal(categoryEvents.length, 4);

        const nestedCategoryResponse = await fetch(`${fixture.baseUrl}/streams/category/orders/eu`);
        assert.equal(nestedCategoryResponse.status, 200);
        const nestedCategoryEvents = await parseNdjson(nestedCategoryResponse);
        assert.deepEqual(nestedCategoryEvents.map(event => event.stream), ['orders/eu/1', 'orders/eu/2']);
    } finally {
        await destroyFixture(fixture);
    }
});

test('GET /query returns NDJSON and exposes a serialized commit condition header', async () => {
    const fixture = await createFixture();
    try {
        await commitAsync(fixture.eventStore, 'orders-1', [
            { type: 'OrderPlaced', orderId: '1' },
            { type: 'OrderPlaced', orderId: '2' }
        ]);

        const filter = encodeURIComponent(JSON.stringify({ payload: { orderId: '2' } }));
        const response = await fetch(`${fixture.baseUrl}/query?types=OrderPlaced&filter=${filter}`);
        assert.equal(response.status, 200);
        const condition = JSON.parse(response.headers.get('x-event-store-query-condition'));
        assert.deepEqual(condition, {
            types: ['OrderPlaced'],
            noneMatchAfter: 2,
            matcher: { payload: { orderId: '2' } }
        });

        const events = await parseNdjson(response);
        assert.equal(events.length, 1);
        assert.equal(events[0].payload.orderId, '2');
    } finally {
        await destroyFixture(fixture);
    }
});

test('PUT /consumers/:identifier/stream/:stream and GET /consumers endpoints expose durable consumers', async () => {
    const fixture = await createFixture();
    try {
        const streamResponse = await fetch(`${fixture.baseUrl}/streams/orders-1`, {
            method: 'PUT',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ stream: 'orders-1' })
        });
        assert.equal(streamResponse.status, 201);

        const createResponse = await fetch(`${fixture.baseUrl}/consumers/orders-reader/stream/orders-1/from/1`, {
            method: 'PUT',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ lastSeen: null })
        });
        assert.equal(createResponse.status, 201);
        assert.deepEqual(await createResponse.json(), {
            identifier: 'orders-reader',
            stream: 'orders-1',
            position: 1,
            state: { lastSeen: null }
        });

        const consumerResponse = await fetch(`${fixture.baseUrl}/consumers/orders-reader`);
        assert.equal(consumerResponse.status, 200);
        assert.deepEqual(await consumerResponse.json(), {
            name: 'stream-orders-1.orders-reader',
            identifier: 'orders-reader',
            stream: 'orders-1',
            position: 1,
            state: { lastSeen: null }
        });

        const listResponse = await fetch(`${fixture.baseUrl}/consumers`);
        assert.equal(listResponse.status, 200);
        const list = await listResponse.json();
        assert.deepEqual(list, {
            consumers: [
                {
                    name: 'stream-orders-1.orders-reader',
                    identifier: 'orders-reader',
                    stream: 'orders-1'
                }
            ]
        });
    } finally {
        await destroyFixture(fixture);
    }
});
