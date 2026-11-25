import expect from 'expect.js';
import fs from 'fs';
import path from 'path';
import SyncScheduler from '../src/SyncScheduler.js';
import FileHandlePool from '../src/FileHandlePool.js';
import { drained, waitFor } from './syncSpy.js';
import { fileURLToPath } from 'url';

const __dirname = fileURLToPath(new URL('.', import.meta.url));
const dataDirectory = path.join(__dirname, 'data', 'sync-scheduler');

describe('SyncScheduler', function() {

    let files, opened, originalOpen, releases, failWith;

    beforeEach(function() {
        fs.rmSync(dataDirectory, { recursive: true, force: true });
        fs.mkdirSync(dataDirectory, { recursive: true });
        // Not open in the pool, so every sync opens the file itself and can be held below.
        const fileHandlePool = new FileHandlePool();
        files = ['a', 'b', 'c'].map(name => {
            const fileName = path.join(dataDirectory, name);
            fs.writeFileSync(fileName, name);
            return { fileName, fileHandlePool };
        });
        opened = [];
        releases = [];
        failWith = null;
        // Hold every sync until the test releases it, to observe a run while it is in flight.
        originalOpen = fs.open;
        fs.open = (fileName, flags, callback) => {
            opened.push(fileName);
            releases.push(() => {
                if (failWith) {
                    callback(Object.assign(new Error(failWith), { code: failWith }));
                } else {
                    originalOpen(fileName, flags, callback);
                }
            });
        };
    });

    afterEach(function() {
        fs.open = originalOpen;
        fs.rmSync(dataDirectory, { recursive: true, force: true });
    });

    function release() {
        releases.splice(0).forEach(resolve => resolve());
    }

    function settle() {
        return new Promise(resolve => setTimeout(resolve, 20));
    }

    it('invokes the callbacks right away without a sync setting', function() {
        const scheduler = new SyncScheduler();
        let acknowledged = false;
        scheduler.flushed(files[0], [() => (acknowledged = true)]);
        scheduler.created(files[1]);
        expect(acknowledged).to.be(true);
        expect(scheduler.running).to.be(false);
    });

    it('syncs all files of a run in parallel in the background', async function() {
        const scheduler = new SyncScheduler({ syncInterval: 1 });
        const acknowledged = [];
        files.forEach((file, i) => scheduler.flushed(file, [() => acknowledged.push(i)]));
        await waitFor(() => opened.length === files.length);
        // All syncs started before any of them finished
        expect(opened).to.eql(files.map(file => file.fileName));
        expect(acknowledged).to.eql([]);
        release();
        await drained(scheduler);
        expect(acknowledged).to.eql([0, 1, 2]);
    });

    it('syncs a new file together with its directory', async function() {
        const scheduler = new SyncScheduler({ syncInterval: 1 });
        scheduler.created(files[0]);
        await waitFor(() => opened.length === 2);
        expect(opened).to.eql([files[0].fileName, dataDirectory]);
        release();
        await drained(scheduler);
    });

    it('leaves writes during a run to the next run', async function() {
        const scheduler = new SyncScheduler({ syncInterval: 1 });
        const acknowledged = [];
        scheduler.flushed(files[0], [() => acknowledged.push(0)]);
        await waitFor(() => opened.length === 1);
        scheduler.flushed(files[1], [() => acknowledged.push(1)]);
        await settle();
        // The first run is still syncing, so the second one has not started yet
        expect(opened).to.eql([files[0].fileName]);
        release();
        await waitFor(() => opened.length === 2);
        expect(acknowledged).to.eql([0]);
        expect(opened).to.eql([files[0].fileName, files[1].fileName]);
        release();
        await drained(scheduler);
        expect(acknowledged).to.eql([0, 1]);
    });

    it('syncs the flushes of one tick together with syncOnFlush', async function() {
        const scheduler = new SyncScheduler({ syncOnFlush: true });
        const acknowledged = [];
        scheduler.flushed(files[0], [() => acknowledged.push(0)]);
        scheduler.flushed(files[1], [() => acknowledged.push(1)]);
        await waitFor(() => opened.length === 2);
        expect(opened).to.eql([files[0].fileName, files[1].fileName]);
        release();
        await drained(scheduler);
        expect(acknowledged).to.eql([0, 1]);
    });

    it('syncs everything waiting without waiting for the interval on drain()', async function() {
        const scheduler = new SyncScheduler({ syncInterval: 60000 });
        const acknowledged = [];
        scheduler.flushed(files[0], [() => acknowledged.push(0)]);
        const draining = drained(scheduler);
        await waitFor(() => opened.length === 1);
        expect(opened).to.eql([files[0].fileName]);
        scheduler.flushed(files[1], [() => acknowledged.push(1)]);
        release();
        await waitFor(() => opened.length === 2);
        // Writes during the run are drained as well
        expect(opened).to.eql([files[0].fileName, files[1].fileName]);
        release();
        await draining;
        expect(acknowledged).to.eql([0, 1]);
    });

    it('acknowledges a file that was deleted before it was synced', async function() {
        const scheduler = new SyncScheduler({ syncInterval: 1 });
        let acknowledged = false;
        scheduler.flushed(files[0], [() => (acknowledged = true)]);
        failWith = 'ENOENT';
        await waitFor(() => opened.length === 1);
        release();
        await drained(scheduler);
        expect(acknowledged).to.be(true);
    });

    it('passes a failed sync to drain() and never acknowledges when a sync fails', async function() {
        const scheduler = new SyncScheduler({ syncInterval: 1 });
        let acknowledged = false;
        scheduler.flushed(files[0], [() => (acknowledged = true)]);
        const draining = drained(scheduler);
        failWith = 'EIO';
        await waitFor(() => opened.length === 1);
        release();
        try {
            await draining;
            expect().fail('drain() should call back with the error');
        } catch (error) {
            expect(error.code).to.be('EIO');
        }
        expect(acknowledged).to.be(false);
    });

    it('throws a failed sync as an uncaught exception without anybody draining', async function() {
        const scheduler = new SyncScheduler({ syncInterval: 1 });
        let acknowledged = false;
        scheduler.flushed(files[0], [() => (acknowledged = true)]);
        failWith = 'EIO';
        await waitFor(() => opened.length === 1);
        const listeners = process.listeners('uncaughtException');
        process.removeAllListeners('uncaughtException');
        const uncaught = new Promise(resolve => process.once('uncaughtException', resolve));
        release();
        const error = await uncaught;
        listeners.forEach(listener => process.on('uncaughtException', listener));
        expect(error.code).to.be('EIO');
        expect(acknowledged).to.be(false);
        expect(scheduler.running).to.be(false);
    });
});
