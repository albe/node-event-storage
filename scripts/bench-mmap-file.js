import fs from 'fs-extra';
import { hrtime } from 'process';
import AppendOnlyMmapedFile from '../src/AppendOnlyMmapedFile.js';

const dataDirectory = 'data/bench-mmap';
const writeCount = 200000;

function measure(name, payload) {
    const file = new AppendOnlyMmapedFile(`${name}.mapped`, {
        dataDirectory,
        writeBufferSize: 64 * 1024
    });
    file.open();
    const start = hrtime.bigint();
    for (let index = 0; index < writeCount; index++) {
        file.write(payload);
    }
    file.flush();
    const end = hrtime.bigint();
    file.close();

    const seconds = Number(end - start) / 1e9;
    const throughput = Math.round(writeCount / seconds);
    const mbPerSecond = ((payload.byteLength * writeCount) / (1024 * 1024 * seconds)).toFixed(2);
    return { throughput, mbPerSecond, seconds: seconds.toFixed(3) };
}

fs.emptyDirSync(dataDirectory);

const indexPayload = Buffer.alloc(16, 7);
const storagePayload = Buffer.from(JSON.stringify({
    doc: 'this is some test document for measuring performance',
    value: 123.45,
    amount: 999,
    pad: ' '.repeat(32)
}));

const indexResult = measure('index-16-byte', indexPayload);
const storageResult = measure('storage-like', storagePayload);

console.log('AppendOnlyMmapedFile throughput benchmark');
console.log(`index-like 16B writes: ${indexResult.throughput}/s (${indexResult.mbPerSecond} MiB/s, ${indexResult.seconds}s)`);
console.log(`storage-like ${storagePayload.byteLength}B writes: ${storageResult.throughput}/s (${storageResult.mbPerSecond} MiB/s, ${storageResult.seconds}s)`);
