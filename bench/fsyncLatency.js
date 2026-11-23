/*
 * Counts fsyncs and adds a simulated latency to each of them, for the blocking calls (`fs.fsyncSync`,
 * `fs.fdatasyncSync`) as well as for those in the background (`FileHandle#sync`, `FileHandle#datasync`).
 * Background syncs run on the libuv thread pool, so at most `UV_THREADPOOL_SIZE` (default 4) of them wait at once.
 */
import fs from 'fs';
import os from 'os';
import path from 'path';

export const stats = { fsyncs: 0, blockedTime: 0 };

export function reset() {
    stats.fsyncs = 0;
    stats.blockedTime = 0;
}

export async function simulateFsyncLatency(latency) {
    const sleeper = new Int32Array(new SharedArrayBuffer(4));
    for (const name of ['fsyncSync', 'fdatasyncSync']) {
        const original = fs[name];
        fs[name] = (fd) => {
            const start = performance.now();
            stats.fsyncs++;
            try {
                if (latency > 0) Atomics.wait(sleeper, 0, 0, latency);
                return original(fd);
            } finally {
                stats.blockedTime += performance.now() - start;
            }
        };
    }

    const threads = Number(process.env.UV_THREADPOOL_SIZE || 4);
    let busy = 0;
    const queue = [];
    const acquire = () => busy < threads ? (busy++, Promise.resolve()) : new Promise(resolve => queue.push(resolve));
    const releaseThread = () => queue.length ? queue.shift()() : busy--;

    const probe = path.join(os.tmpdir(), `fsync-latency-probe-${process.pid}`);
    const handle = await fs.promises.open(probe, 'w');
    const prototype = Object.getPrototypeOf(handle);
    await handle.close();
    fs.rmSync(probe, { force: true });
    for (const name of ['sync', 'datasync']) {
        const original = prototype[name];
        prototype[name] = async function() {
            stats.fsyncs++;
            await acquire();
            try {
                if (latency > 0) await new Promise(resolve => setTimeout(resolve, latency));
                return await original.call(this);
            } finally {
                releaseThread();
            }
        };
    }
}
