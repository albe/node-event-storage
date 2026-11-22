import fs from 'fs';
import os from 'os';
import path from 'path';
import { assert } from '../utils/util.js';
import { syncFile, syncDirectory } from '../utils/fsUtil.js';

const LOCK_RECLAIM = 0x1;
const LOCK_THROW = 0x2;
const LOCK_AUTO = 0x4;

// Ends in `.lock` so that partition scans, also those of earlier versions, skip it like the lock directory itself.
const OWNER_FILE = 'owner.lock';
const RECLAIM_MARKER = 'reclaim.lock';
const CHECKPOINT_FILE = 'checkpoint.lock';

class StorageLockedError extends Error {}

/**
 * Containers sharing a host name can not see each other's processes, so a pid is only comparable within one pid namespace.
 *
 * @returns {string|null} The pid namespace of this process on Linux, null elsewhere.
 */
function currentPidNamespace() {
    try {
        return fs.readlinkSync('/proc/self/ns/pid');
    /* c8 ignore next 3 */
    } catch (e) {
        return null;
    }
}

/**
 * @param {number} pid
 * @returns {boolean}
 */
function isProcessRunning(pid) {
    try {
        process.kill(pid, 0);
        return true;
    } catch (e) {
        return e.code === 'EPERM';
    }
}

/**
 * The single-writer lock of a storage: a directory that only one writer can create, holding a file that names its owner
 * process, so that the lock of a crashed writer can be told apart from the lock of a running one.
 */
class StorageLock {

    /**
     * @param {string} directory The path of the lock directory.
     * @param {string} storageName The storage name used in error messages.
     * @param {number} [mode] One of the LOCK_* constants. Default LOCK_AUTO.
     */
    constructor(directory, storageName, mode = LOCK_AUTO) {
        this.directory = directory;
        this.storageName = storageName;
        this.mode = mode;
        this.held = false;
    }

    /**
     * Remove a lock left behind by another writer: always with LOCK_RECLAIM, with LOCK_AUTO only when its owner is gone.
     *
     * @returns {boolean} True if such a lock was removed, which means the storage needs to be repaired.
     */
    reclaimOrphaned() {
        if (this.held) {
            return false;
        }
        if (this.mode === LOCK_RECLAIM) {
            return this.release();
        }
        return this.mode === LOCK_AUTO && this.isOwnerGone() && this.removeStaleLock();
    }

    /**
     * Two writers starting at the same time may both find the owner gone. Without a claim, the slower one would delete
     * the lock the faster one has just acquired. The claim is a directory inside the stale lock that only one of them
     * can create, and the owner is checked again while holding it.
     *
     * @private
     * @returns {boolean} True if the stale lock was removed by this instance.
     */
    removeStaleLock() {
        const marker = path.join(this.directory, RECLAIM_MARKER);
        try {
            fs.mkdirSync(marker);
        } catch (e) {
            return false;
        }
        if (!this.isOwnerGone()) {
            fs.rmdirSync(marker);
            return false;
        }
        fs.rmSync(this.directory, { recursive: true, force: true });
        return true;
    }

    /**
     * @returns {boolean} True if the lock was acquired or false if this instance already holds it.
     * @throws {StorageLockedError} If another writer holds the lock.
     */
    acquire() {
        if (this.held) {
            return false;
        }
        try {
            fs.mkdirSync(this.directory);
        } catch (e) {
            /* c8 ignore next */
            assert(e.code === 'EEXIST', `Error creating lock for storage ${this.storageName}: ` + e.message);
            throw new StorageLockedError(this.describeHolder());
        }
        this.held = true;
        StorageLock.heldByThisProcess.add(this.directory);
        const owner = { pid: process.pid, hostname: os.hostname(), pidNamespace: currentPidNamespace(), lockedAt: new Date().toISOString() };
        fs.writeFileSync(path.join(this.directory, OWNER_FILE), JSON.stringify(owner));
        return true;
    }

    /**
     * Remove the lock, no matter which writer holds it.
     *
     * @returns {boolean} True if a lock not held by this instance was removed.
     */
    release() {
        const exists = fs.existsSync(this.directory);
        const orphaned = exists && !this.held;
        if (exists) {
            fs.rmSync(this.directory, { recursive: true, force: true });
        }
        if (this.held) {
            StorageLock.heldByThisProcess.delete(this.directory);
        }
        this.held = false;
        return orphaned;
    }

    /**
     * Record up to which global position all indexes are on disk. It belongs to the lock, because only the repair after
     * a crash, which finds the lock left behind, needs it.
     *
     * @param {number} position
     */
    writeCheckpoint(position) {
        const fileName = path.join(this.directory, CHECKPOINT_FILE);
        const tmpFileName = fileName + '.tmp';
        fs.writeFileSync(tmpFileName, String(position));
        syncFile(tmpFileName);
        fs.renameSync(tmpFileName, fileName);
        syncDirectory(this.directory);
    }

    /**
     * @returns {number} The position up to which all indexes are on disk, 0 if the lock holds no checkpoint.
     */
    readCheckpoint() {
        try {
            const position = Number(fs.readFileSync(path.join(this.directory, CHECKPOINT_FILE), 'utf8'));
            return Number.isInteger(position) && position > 0 ? position : 0;
        } catch (e) {
            return 0;
        }
    }

    /**
     * @returns {{pid: number, hostname: string, pidNamespace: string|null, lockedAt: string}|null} The process holding
     *   the lock, or null if there is no lock or it carries no owner information.
     */
    readOwner() {
        try {
            const owner = JSON.parse(fs.readFileSync(path.join(this.directory, OWNER_FILE), 'utf8'));
            return Number.isInteger(owner.pid) ? owner : null;
        } catch (e) {
            return null;
        }
    }

    /**
     * Only an owner that provably ran in this process table (same host, same pid namespace) and no longer runs counts
     * as gone. Anything else, including a lock without owner information, is treated as held.
     *
     * @private
     * @returns {boolean}
     */
    isOwnerGone() {
        const owner = this.readOwner();
        if (owner === null || owner.hostname !== os.hostname() || owner.pidNamespace !== currentPidNamespace()) {
            return false;
        }
        if (owner.pid === process.pid) {
            return !StorageLock.heldByThisProcess.has(this.directory);
        }
        return !isProcessRunning(owner.pid);
    }

    /**
     * @private
     * @returns {string}
     */
    describeHolder() {
        const owner = this.readOwner();
        const holder = owner === null ? 'another process' : `process ${owner.pid} on ${owner.hostname} (locked at ${owner.lockedAt})`;
        return `Storage ${this.storageName} is locked by ${holder}. If that process is no longer running, open the storage with LOCK_RECLAIM.`;
    }
}

// Lock directories held in this process, to tell an own live lock from one left by an earlier process with the same pid.
StorageLock.heldByThisProcess = new Set();

export default StorageLock;
export { StorageLockedError, LOCK_THROW, LOCK_RECLAIM, LOCK_AUTO };
