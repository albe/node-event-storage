import expect from 'expect.js';
import fs from 'fs';
import path from 'path';
import SyncScheduler from '../src/SyncScheduler.js';
import FileHandlePool from '../src/FileHandlePool.js';
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
        originalOpen = fs.promises.open;
        fs.promises.open = async (fileName, flags) => {
            opened.push(fileName);
            await new Promise(resolve => releases.push(resolve));
            if (failWith) {
                throw Object.assign(new Error(failWith), { code: failWith });
            }
            return originalOpen(fileName, flags);
        };
    });

    afterEach(function() {
        fs.promises.open = originalOpen;
        fs.rmSync(dataDirectory, { recursive: true, force: true });
    });

    function release() {
        releases.splice(0).forEach(resolve => resolve());
    }

    function settle() {
        return new Promise(resolve => setTimeout(resolve, 20));
    }

    async function until(condition) {
        while (!condition()) {
            await new Promise(resolve => setTimeout(resolve, 1));
        }
    }

    it('invokes the callbacks right away without a sync setting', function() {
        const scheduler = new SyncScheduler();
        let acknowledged = false;
        scheduler.flushed(files[0], [() => (acknowledged = true)]);
        scheduler.created(files[1]);
        expect(acknowledged).to.be(true);
        expect(scheduler.loop).to.be(null);
    });

    it('syncs all files of a run in parallel in the background', async function() {
        const scheduler = new SyncScheduler({ syncInterval: 1 });
        const acknowledged = [];
        files.forEach((file, i) => scheduler.flushed(file, [() => acknowledged.push(i)]));
        await until(() => opened.length === files.length);
        // All syncs started before any of them finished
        expect(opened).to.eql(files.map(file => file.fileName));
        expect(acknowledged).to.eql([]);
        release();
        await scheduler.drain();
        expect(acknowledged).to.eql([0, 1, 2]);
    });

    it('syncs a new file together with its directory', async function() {
        const scheduler = new SyncScheduler({ syncInterval: 1 });
        scheduler.created(files[0]);
        await until(() => opened.length === 2);
        expect(opened).to.eql([files[0].fileName, dataDirectory]);
        release();
        await scheduler.drain();
    });

    it('leaves writes during a run to the next run', async function() {
        const scheduler = new SyncScheduler({ syncInterval: 1 });
        const acknowledged = [];
        scheduler.flushed(files[0], [() => acknowledged.push(0)]);
        await until(() => opened.length === 1);
        scheduler.flushed(files[1], [() => acknowledged.push(1)]);
        await settle();
        // The first run is still syncing, so the second one has not started yet
        expect(opened).to.eql([files[0].fileName]);
        release();
        await until(() => opened.length === 2);
        expect(acknowledged).to.eql([0]);
        expect(opened).to.eql([files[0].fileName, files[1].fileName]);
        release();
        await scheduler.drain();
        expect(acknowledged).to.eql([0, 1]);
    });

    it('syncs the flushes of one tick together with syncOnFlush', async function() {
        const scheduler = new SyncScheduler({ syncOnFlush: true });
        const acknowledged = [];
        scheduler.flushed(files[0], [() => acknowledged.push(0)]);
        scheduler.flushed(files[1], [() => acknowledged.push(1)]);
        await until(() => opened.length === 2);
        expect(opened).to.eql([files[0].fileName, files[1].fileName]);
        release();
        await scheduler.drain();
        expect(acknowledged).to.eql([0, 1]);
    });

    it('syncs everything waiting without waiting for the interval on drain()', async function() {
        const scheduler = new SyncScheduler({ syncInterval: 60000 });
        const acknowledged = [];
        scheduler.flushed(files[0], [() => acknowledged.push(0)]);
        const drained = scheduler.drain();
        await until(() => opened.length === 1);
        expect(opened).to.eql([files[0].fileName]);
        scheduler.flushed(files[1], [() => acknowledged.push(1)]);
        release();
        await until(() => opened.length === 2);
        // Writes during the run are drained as well
        expect(opened).to.eql([files[0].fileName, files[1].fileName]);
        release();
        await drained;
        expect(acknowledged).to.eql([0, 1]);
    });

    it('acknowledges a file that was deleted before it was synced', async function() {
        const scheduler = new SyncScheduler({ syncInterval: 1 });
        let acknowledged = false;
        scheduler.flushed(files[0], [() => (acknowledged = true)]);
        failWith = 'ENOENT';
        await until(() => opened.length === 1);
        release();
        await scheduler.drain();
        expect(acknowledged).to.be(true);
    });

    it('rejects drain() and never acknowledges when a sync fails', async function() {
        const scheduler = new SyncScheduler({ syncInterval: 1 });
        let acknowledged = false;
        scheduler.flushed(files[0], [() => (acknowledged = true)]);
        const drained = scheduler.drain();
        failWith = 'EIO';
        await until(() => opened.length === 1);
        release();
        try {
            await drained;
            expect().fail('drain() should reject');
        } catch (error) {
            expect(error.code).to.be('EIO');
        }
        expect(acknowledged).to.be(false);
    });
});
