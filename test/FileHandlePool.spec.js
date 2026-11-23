import expect from 'expect.js';
import fs from 'fs-extra';
import fsSync from 'fs';
import FileHandlePool from '../src/FileHandlePool.js';
import { fileURLToPath } from 'url';

const __dirname = fileURLToPath(new URL('.', import.meta.url));
const dataDirectory = fileURLToPath(new URL('./data', import.meta.url));

describe('FileHandlePool', function() {

    let pool;
    let targets;

    beforeEach(function() {
        fs.emptyDirSync(dataDirectory);
        pool = null;
        targets = [];
    });

    afterEach(function() {
        if (pool) {
            for (const target of targets) pool.evict(target, false);
        }
        fs.emptyDirSync(dataDirectory);
    });

    function createTarget(name, overrides = {}) {
        const target = Object.assign({
            fileName: dataDirectory + '/' + name,
            fileMode: 'a+'
        }, overrides);
        targets.push(target);
        return target;
    }

    it('updates the LRU order when touching an open target', function() {
        pool = new FileHandlePool(2);
        const target1 = createTarget('one');
        const target2 = createTarget('two');
        const target3 = createTarget('three');

        pool.get(target1);
        pool.get(target2);
        pool.touch(target1);
        pool.get(target3);

        expect(pool.has(target1)).to.be(true);
        expect(pool.has(target2)).to.be(false);
        expect(pool.has(target3)).to.be(true);
    });

    it('returns false when evicting an unknown target and ignores missing touches', function() {
        pool = new FileHandlePool(1);
        const target = createTarget('missing');

        expect(() => pool.touch(target)).to.not.throwError();
        expect(pool.evict(target)).to.be(false);
    });

    it('passes evicted=false to the before-close hook on explicit close', function() {
        pool = new FileHandlePool(1);
        const closeCalls = [];
        const target = createTarget('close', {
            onBeforeClose(fd, evicted) {
                closeCalls.push({ fd, evicted });
            }
        });

        const fd = pool.get(target);

        expect(pool.evict(target, false)).to.be(true);
        expect(closeCalls).to.eql([{ fd, evicted: false }]);
        expect(pool.openCount).to.be(0);
    });

    describe('sync', function() {

        let fsyncs, opened, closed, originalFsync, originalOpen, originalClose, hold;

        beforeEach(function() {
            fsyncs = [];
            opened = [];
            closed = [];
            hold = null;
            originalFsync = fsSync.fsync;
            originalOpen = fsSync.promises.open;
            originalClose = fsSync.closeSync;
            fsSync.fsync = (fd, callback) => {
                fsyncs.push(fd);
                const sync = () => originalFsync(fd, callback);
                if (hold) hold.push(sync); else sync();
            };
            fsSync.promises.open = (fileName, flags) => {
                opened.push(fileName);
                return originalOpen(fileName, flags);
            };
            fsSync.closeSync = fd => {
                closed.push(fd);
                return originalClose(fd);
            };
        });

        afterEach(function() {
            fsSync.fsync = originalFsync;
            fsSync.promises.open = originalOpen;
            fsSync.closeSync = originalClose;
        });

        it('syncs an open file through its pooled descriptor', async function() {
            pool = new FileHandlePool();
            const target = createTarget('open');
            const fd = pool.get(target);
            await pool.sync(target);
            expect(fsyncs).to.eql([fd]);
            expect(opened).to.eql([]);
            expect(closed).to.eql([]);
        });

        it('closes a descriptor evicted during its sync only once the sync is done', async function() {
            pool = new FileHandlePool();
            const target = createTarget('evicted');
            const fd = pool.get(target);
            hold = [];
            const syncing = pool.sync(target);
            pool.evict(target);
            expect(pool.has(target)).to.be(false);
            expect(closed).to.eql([]);
            // A new descriptor for the same file does not wait for the sync
            const reopened = pool.get(target);
            expect(reopened).not.to.be(fd);
            hold.splice(0).forEach(sync => sync());
            await syncing;
            expect(closed).to.eql([fd]);
        });

        it('syncs a file that is not open through its own descriptor', async function() {
            pool = new FileHandlePool();
            const target = createTarget('closed');
            fs.writeFileSync(target.fileName, 'x');
            await pool.sync(target);
            expect(opened).to.eql([target.fileName]);
            expect(pool.has(target)).to.be(false);
        });

        it('skips a file deleted before its sync', async function() {
            pool = new FileHandlePool();
            const target = createTarget('deleted');
            await pool.sync(target);
            expect(opened).to.eql([target.fileName]);
        });

        it('rejects when the fsync fails and still closes an evicted descriptor', async function() {
            pool = new FileHandlePool();
            const target = createTarget('failing');
            const fd = pool.get(target);
            fsSync.fsync = (fd, callback) => {
                pool.evict(target);
                callback(Object.assign(new Error('EIO'), { code: 'EIO' }));
            };
            try {
                await pool.sync(target);
                expect().fail('sync() should reject');
            } catch (error) {
                expect(error.code).to.be('EIO');
            }
            expect(closed).to.eql([fd]);
        });
    });

});
