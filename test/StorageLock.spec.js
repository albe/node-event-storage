import expect from 'expect.js';
import fs from 'fs-extra';
import os from 'os';
import path from 'path';
import { spawnSync } from 'child_process';
import StorageLock, { StorageLockedError, LOCK_AUTO, LOCK_THROW, LOCK_RECLAIM } from '../src/Storage/StorageLock.js';
import { fileURLToPath } from 'url';

const __dirname = fileURLToPath(new URL('.', import.meta.url));
const dataDirectory = __dirname + 'data';
const lockDirectory = path.join(dataDirectory, 'storage.lock');

describe('StorageLock', function() {

    let locks = [];

    beforeEach(function() {
        fs.emptyDirSync(dataDirectory);
    });

    afterEach(function() {
        locks.forEach(lock => lock.held && lock.release());
        locks = [];
    });

    function createLock(mode) {
        const lock = new StorageLock(lockDirectory, 'storage', mode);
        locks.push(lock);
        return lock;
    }

    function leaveLock(owner) {
        fs.mkdirSync(lockDirectory);
        if (owner) {
            const pidNamespace = (() => { try { return fs.readlinkSync('/proc/self/ns/pid'); } catch (e) { return null; } })();
            fs.writeFileSync(path.join(lockDirectory, 'owner.lock'), JSON.stringify(Object.assign({
                hostname: os.hostname(), pidNamespace, lockedAt: new Date().toISOString()
            }, owner)));
        }
    }

    function deadPid() {
        return spawnSync(process.execPath, ['-e', '']).pid;
    }

    it('defaults to LOCK_AUTO', function() {
        expect(createLock().mode).to.be(LOCK_AUTO);
    });

    it('acquires and releases the lock', function() {
        const lock = createLock();
        expect(lock.acquire()).to.be(true);
        expect(lock.held).to.be(true);
        expect(lock.acquire()).to.be(false);
        expect(fs.existsSync(lockDirectory)).to.be(true);
        expect(lock.release()).to.be(false);
        expect(lock.held).to.be(false);
        expect(fs.existsSync(lockDirectory)).to.be(false);
    });

    it('records the owner process', function() {
        const lock = createLock();
        lock.acquire();
        const owner = lock.readOwner();
        expect(owner.pid).to.be(process.pid);
        expect(owner.hostname).to.be(os.hostname());
        expect(owner.lockedAt).to.be.a('string');
    });

    it('throws when another instance holds the lock', function() {
        createLock().acquire();
        expect(() => createLock().acquire()).to.throwError(e => {
            expect(e).to.be.a(StorageLockedError);
            expect(e.message).to.contain(`process ${process.pid}`);
        });
    });

    it('does not reclaim a lock held in this process', function() {
        createLock().acquire();
        expect(createLock().reclaimOrphaned()).to.be(false);
    });

    it('reclaims the lock of a process that is gone', function() {
        leaveLock({ pid: deadPid() });
        const lock = createLock();
        expect(lock.reclaimOrphaned()).to.be(true);
        expect(lock.acquire()).to.be(true);
    });

    it('reclaims a lock left by an earlier process with the same pid', function() {
        leaveLock({ pid: process.pid });
        expect(createLock().reclaimOrphaned()).to.be(true);
    });

    it('does not reclaim the lock of a running process', function() {
        leaveLock({ pid: process.ppid });
        const lock = createLock();
        expect(lock.reclaimOrphaned()).to.be(false);
        expect(() => lock.acquire()).to.throwError(e => {
            expect(e).to.be.a(StorageLockedError);
            expect(e.message).to.contain(`process ${process.ppid}`);
        });
    });

    it('does not reclaim the lock of a process on another host or in another pid namespace', function() {
        leaveLock({ pid: deadPid(), hostname: 'other-host' });
        expect(createLock().reclaimOrphaned()).to.be(false);
        fs.rmSync(lockDirectory, { recursive: true });
        leaveLock({ pid: deadPid(), pidNamespace: 'pid:[1]' });
        expect(createLock().reclaimOrphaned()).to.be(false);
    });

    it('does not reclaim a lock without owner information', function() {
        leaveLock();
        const lock = createLock();
        expect(lock.readOwner()).to.be(null);
        expect(lock.reclaimOrphaned()).to.be(false);
        expect(() => lock.acquire()).to.throwError(e => {
            expect(e).to.be.a(StorageLockedError);
            expect(e.message).to.contain('another process');
            expect(e.message).to.contain('LOCK_RECLAIM');
        });
    });

    it('never reclaims with LOCK_THROW', function() {
        leaveLock({ pid: deadPid() });
        expect(createLock(LOCK_THROW).reclaimOrphaned()).to.be(false);
    });

    it('always reclaims with LOCK_RECLAIM', function() {
        leaveLock();
        expect(createLock(LOCK_RECLAIM).reclaimOrphaned()).to.be(true);
    });
});
