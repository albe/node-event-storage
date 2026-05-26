# HTTP layer benchmarks

`bench-http-layer.js` measures client-observed throughput of the HTTP layer over local loopback.
The request generator runs in a separate process, so the reported numbers include the HTTP round-trip on `127.0.0.1` and act as a lower bound for real network deployments.

## Included measurements

- write throughput in events/s with exactly 1 event per commit
  - once with requests spread across per-lane streams
  - once with all requests targeting a single shared stream
- read throughput in events/s for the same scenarios as the base package read benchmark:
  - forward full scan
  - backwards full scan
  - join stream
  - range scan

Concurrency is measured at `1`, `2`, `4`, `8`, and `16`, and rendered as table columns.

## Run

```bash
npm run bench:http
```

```bash
npm run bench:http:write
```

## Optional environment variables

```bash
HTTP_BENCH_READ_EVENTS=10000
HTTP_BENCH_READ_REQUESTS_PER_LANE=4
HTTP_BENCH_WRITE_REQUESTS_PER_LANE=400
npm run bench:http
```


