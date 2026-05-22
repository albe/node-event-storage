# event-storage-http

HTTP API layer for `node-event-storage`.

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
- `GET /consumers`

Stream, join, category, and query reads return `application/x-ndjson`. These endpoints use the core EventStore raw mode, so event documents are streamed as newline-delimited JSON buffers directly to the HTTP response.

Query responses also expose a serialized optimistic-concurrency condition in the `x-event-store-query-condition` response header so clients can pass it back to `POST /streams/{stream}/commit`.

`start` and `end` are accepted wherever a revision boundary is expected. Matchers are JSON object matchers using the same shape as the core storage matchers (`{ stream, payload, metadata }`).

Raw-mode matcher notes:

- Object matchers are evaluated against compact JSON bytes (no parsing in the HTTP layer).
- Function matchers in raw mode receive a raw document `Buffer`.
- Raw object matchers require the default compact JSON serializer format.

## Usage

```js
import EventStore from 'event-storage';
import EventStoreHttpApi from './src/EventStoreHttpApi.js';

const eventStore = new EventStore({
    storageDirectory: './data',
    typeAccessor: 'type'
});

const api = new EventStoreHttpApi(eventStore);
api.listen(3000);
```

## Async read prototype

`src/AsyncReadablePartition.js` contains a forward-read prototype that uses async file-handle methods. It is intended for read-heavy HTTP workloads where overlapping disk reads can matter more than single-call latency.

Run `npm run benchmark` inside `event-storage-http` to compare the synchronous and async partition readers under configurable concurrent scan load.
