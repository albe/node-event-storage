import expect from 'expect.js';
import fs from 'fs';
import path from 'path';
import SyncScheduler from '../src/SyncScheduler.js';
import FileHandlePool from '../src/FileHandlePool.js';
import { fileURLToPath } from 'url';

const __dirname = fileURLToPath(new URL('.', import.meta.url));
const dataDirectory = path.join(__dirname, 'data', 'sync-scheduler');

describe('SyncScheduler', function() {

    let files, syncSync, opened, originalOpen, originalFsync, releases, failWith;

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
        syncSync = [];
        opened = [];
        releases = [];
        failWith = null;
        originalFsync = fs.fsyncSync;
        fs.fsyncSync = (fd) => { syncSync.push(fd); return originalFsync(fd); };
        // Hold every background sync until the test releases it, to observe a run while it is in flight.
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
        fs.fsyncSync = originalFsync;
        fs.promises.open = originalOpen;
        fs.rmSync(dataDirectory, { recursive: true, force: true });
    });

    function release() {
        releases.splice(0).forEach(resolve => resolve());
    }

    function settle() {
        return new Promise(resolve => setTimeout(resolve, 20));
    }

    it('syncs all files of a run in parallel in the background', async function() {
        const scheduler = new SyncScheduler({ syncInterval: 1 });
        const acknowledged = [];
        files.forEach((file, i) => scheduler.flushed(file, [() => acknowledged.push(i)]));
        await settle();
        // All syncs started before any of them finished
        expect(opened).to.eql(files.map(file => file.fileName));
        expect(acknowledged).to.eql([]);
        release();
        await settle();
        expect(acknowledged).to.eql([0, 1, 2]);
        expect(syncSync).to.eql([]);
    });

    it('starts the next run only after the running one finished', async function() {
        const scheduler = new SyncScheduler({ syncInterval: 1 });
        const acknowledged = [];
        scheduler.flushed(files[0], [() => acknowledged.push(0)]);
        await settle();
        scheduler.flushed(files[1], [() => acknowledged.push(1)]);
        await settle();
        // The interval of the second flush has elapsed, but the first run is still syncing
        expect(opened).to.eql([files[0].fileName]);
        release();
        await settle();
        expect(acknowledged).to.eql([0]);
        expect(opened).to.eql([files[0].fileName, files[1].fileName]);
        release();
        await settle();
        expect(acknowledged).to.eql([0, 1]);
    });

    it('completes a running background run synchronously on run()', async function() {
        const scheduler = new SyncScheduler({ syncInterval: 1 });
        const acknowledged = [];
        scheduler.flushed(files[0], [() => acknowledged.push(0)]);
        await settle();
        scheduler.flushed(files[1], [() => acknowledged.push(1)]);
        scheduler.created(files[2]);
        scheduler.run();
        // The running file, the waiting file, the new file and its directory
        expect(syncSync.length).to.be(4);
        expect(acknowledged).to.eql([0, 1]);
        release();
        await settle();
        // The background run that run() took over neither acknowledges twice nor starts another run
        expect(acknowledged).to.eql([0, 1]);
        expect(opened).to.eql([files[0].fileName]);
    });

    it('ignores a failed background sync that run() already took over', async function() {
        const scheduler = new SyncScheduler({ syncInterval: 1 });
        scheduler.flushed(files[0]);
        await settle();
        scheduler.run();
        fs.rmSync(files[0].fileName);
        release();
        await settle();
    });

    it('acknowledges a file that was deleted before it was synced', async function() {
        const scheduler = new SyncScheduler({ syncInterval: 1 });
        let acknowledged = false;
        scheduler.flushed(files[0], [() => (acknowledged = true)]);
        await settle();
        fs.rmSync(files[0].fileName);
        release();
        await settle();
        expect(acknowledged).to.be(true);
    });

    it('throws a failed background sync as an uncaught exception', async function() {
        const scheduler = new SyncScheduler({ syncInterval: 1 });
        let acknowledged = false;
        scheduler.flushed(files[0], [() => (acknowledged = true)]);
        await settle();
        failWith = 'EIO';
        const listeners = process.listeners('uncaughtException');
        process.removeAllListeners('uncaughtException');
        const uncaught = new Promise(resolve => process.once('uncaughtException', resolve));
        release();
        const error = await uncaught;
        listeners.forEach(listener => process.on('uncaughtException', listener));
        expect(error.code).to.be('EIO');
        expect(acknowledged).to.be(false);
    });
});
