# event-storage-http

HTTP API layer for [`event-storage`](https://www.npmjs.com/package/event-storage) — exposes an EventStore instance as a set of REST endpoints over NDJSON streaming.

## What and why

`event-storage-http` bridges a Node.js `EventStore` instance and any HTTP client. It is designed for setups where the event store lives in a **dedicated backend service** that multiple consumers — frontends, microservices, serverless functions, or other runtimes — need to reach over the network without a direct Node.js dependency.

**Use this package when:**
- you run the EventStore as a standalone service and trusted backend clients (Python workers, Go services, other Node.js services, …) need to connect remotely
- you need a simple, language-agnostic integration point between internal services without embedding the storage library

**Do not use this package when:**
- your consumers run in the same Node.js process — use `event-storage` directly for best performance and type safety
- you want to expose data to browsers or the public internet — this layer provides no authentication, authorization, or input sanitization and must only be reachable by trusted clients
- you need fine-grained access control, authentication, or schema validation — add those layers yourself before reaching for this package
- low-latency write paths are critical — the HTTP round-trip adds overhead that in-process access avoids

## Usage

Minimal setup — create an EventStore, wrap it in the HTTP API, and start listening:

```js
import EventStore from 'event-storage';
import EventStoreHttpApi from 'event-storage-http';

const eventStore = new EventStore({
    storageDirectory: './data',
    typeAccessor: 'type'
});

const api = new EventStoreHttpApi(eventStore);
api.listen(3000, () => console.log('Event store listening on port 3000'));
```

### With options

The second argument to `EventStoreHttpApi` accepts configuration for consumer behaviour:

```js
import EventStore from 'event-storage';
import EventStoreHttpApi from 'event-storage-http';

const eventStore = new EventStore({
    storageDirectory: './data',
    typeAccessor: 'type'
});

const api = new EventStoreHttpApi(eventStore, {
    // Re-open and register all persisted consumers at boot time so they begin
    // processing immediately without a client issuing a PUT first.
    autoStartConsumers: true,

    // How long (ms) GET /consumers/:id/until/:version waits before responding
    // with 408 Request Timeout.  Defaults to 10 000.
    consumerPollTimeoutMs: 30_000
});

api.listen(3000, () => console.log('Event store listening on port 3000'));
```

## Endpoints

- `POST /streams/{stream}/commit`
- `PUT /streams/{stream}`
- `GET /streams/{stream}[/from/{from}][/until/{until}][/forwards/{amount}][/backwards/{amount}]`
- `GET /streams/{stream}/version`
- `GET /streams/join[/from/{from}][/until/{until}][/forwards/{amount}][/backwards/{amount}]?streams=...`
- `GET /streams/category/{category}[/from/{from}][/until/{until}][/forwards/{amount}][/backwards/{amount}]`
- `GET /query[/from/{revision}]?types=...`
- `PUT /consumers/{identifier}/stream/{stream}[/from/{revision}]`
- `GET /consumers/{identifier}`
- `GET /consumers/{identifier}/until/{minVersion}`
- `GET /consumers`

Stream, join, category, and query reads return `application/x-ndjson`. These endpoints use the core EventStore raw mode, so event documents are streamed as newline-delimited JSON buffers directly to the HTTP response.

Query responses also expose a serialized optimistic-concurrency condition in the `x-event-store-query-condition` response header so clients can pass it back to `POST /streams/{stream}/commit`.

`start` and `end` are accepted wherever a revision boundary is expected. Matchers are JSON object matchers using the same shape as the core storage matchers (`{ stream, payload, metadata }`).

### Consumer endpoints

`PUT /consumers/{identifier}/stream/{stream}[/from/{revision}]` starts a durable consumer that is kept running in memory and registered in the EventStore's internal `consumers` map (keyed by `identifier`). Re-issuing the PUT replaces the existing consumer and restarts from the new handler and state.

`GET /consumers/{identifier}` returns the live position and state of the named consumer from the in-memory registry. Returns `404` if the consumer is not registered.

`GET /consumers/{identifier}/until/{minVersion}` is a long-poll endpoint that blocks until the named consumer's position reaches `minVersion`, then responds with the consumer's current position and state. If the consumer does not advance to `minVersion` within the configured timeout (default 10 s, configurable via `options.consumerPollTimeoutMs`), the server responds with HTTP `408 Request Timeout`. The consumer must be registered in the event store's consumer registry (via `PUT` or by the startup scan) before calling this endpoint.

```http
GET /consumers/orders-reader/until/5
```

```json
{
  "identifier": "orders-reader",
  "stream": "orders",
  "position": 5,
  "state": { "count": 5 }
}
```

`GET /consumers` lists all consumers currently registered in memory. It also fires an asynchronous filesystem scan to keep the registry eventually consistent with consumers created outside of this process.

On startup, `EventStoreHttpApi` calls `eventStore.scanConsumers()` once to pre-populate the consumer registry. Pass `options.autoStartConsumers: true` to open and register all existing consumers on disk at boot time.

Raw-mode matcher notes:

- Object matchers are evaluated against compact JSON bytes (no parsing in the HTTP layer).
- Function matchers in raw mode receive a raw document `Buffer`.
- Raw object matchers require the default compact JSON serializer format.

## Benchmark snapshot (local loopback)

The benchmark script in `bench/bench-http-layer.js` measures client-observed throughput including HTTP round-trips over `127.0.0.1`.
These numbers are a coarse upper bound for the HTTP layer on this machine and can be compared directionally against the in-process benchmark.

Run used for the table below:

- Date: 2026-05-26
- Command: `npm run bench:http`
- Read fixture: 10,000 events
- Requests per lane: write `400`, read `4`

### Write performance (events/s, 1 event/commit)

| Scenario | 1 | 2 | 4 | 8 | 16 |
| --- | ---: | ---: | ---: | ---: | ---: |
| single-event commit (sharded streams) | 809 | 1,422 | 1,731 | 2,109 | 2,389 |
| single-event commit (single stream) | 1,051 | 1,494 | 1,882 | 2,142 | 2,401 |

### Read performance (events/s)

| Scenario | 1 | 2 | 4 | 8 | 16 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 - forward full scan | 107,829 | 122,049 | 133,120 | 139,627 | 138,417 |
| 2 - backwards full scan | 113,233 | 131,419 | 140,503 | 138,803 | 139,791 |
| 3 - join stream | 127,713 | 132,546 | 142,758 | 145,804 | 143,378 |
| 4 - range scan | 97,060 | 117,719 | 131,956 | 138,267 | 137,104 |

## Further reading

Full documentation for the underlying `event-storage` library, including the EventStore API, streams, consumers, DCB concurrency, and performance notes, is available at:

👉 **https://node-event-storage.readthedocs.io/en/latest/**
