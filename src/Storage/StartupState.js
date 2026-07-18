import fs from 'fs';
import path from 'path';
import { hash } from '../utils/util.js';

const STATE_VERSION = 1;

/**
 * Persistent startup manifest used to speed up storage bootstrap.
 */
class StartupState {

    /**
     * @param {string} storageFile
     * @param {string} directory
     * @param {object} [config]
     * @param {boolean} [config.enabled=false]
     * @param {string} [config.fileName]
     */
    constructor(storageFile, directory, config = {}) {
        this.enabled = !!config.enabled;
        this.storageFile = storageFile;
        this.fileName = path.resolve(directory, config.fileName || `${storageFile}.startup-state.json`);
        this.lastState = null;
    }

    /**
     * @param {object} payload
     * @returns {string}
     */
    checksum(payload) {
        return String(hash(JSON.stringify(payload)));
    }

    /**
     * @param {object} raw
     * @returns {boolean}
     */
    isValid(raw) {
        if (!raw || typeof raw !== 'object') return false;
        if (raw.version !== STATE_VERSION) return false;
        if (raw.storageFile !== this.storageFile) return false;
        if (!raw.payload || typeof raw.payload !== 'object') return false;
        return raw.checksum === this.checksum(raw.payload);
    }

    /**
     * @returns {object|null}
     */
    load() {
        if (!this.enabled || !fs.existsSync(this.fileName)) {
            return null;
        }
        try {
            const raw = JSON.parse(fs.readFileSync(this.fileName, 'utf8'));
            if (!this.isValid(raw)) {
                return null;
            }
            this.lastState = raw.payload;
            return raw.payload;
        } catch (e) {
            return null;
        }
    }

    /**
     * @param {object} payload
     */
    save(payload) {
        if (!this.enabled) {
            return;
        }
        const envelope = {
            version: STATE_VERSION,
            storageFile: this.storageFile,
            payload,
            checksum: this.checksum(payload)
        };
        const tmpFile = `${this.fileName}.${process.pid}.${Date.now()}.${Math.random().toString(16).slice(2)}.tmp`;
        fs.writeFileSync(tmpFile, JSON.stringify(envelope));
        fs.renameSync(tmpFile, this.fileName);
        this.lastState = payload;
    }

    /**
     * Mark the state as dirty before mutating on-disk layout.
     */
    markDirty() {
        if (!this.enabled) {
            return;
        }
        const base = this.lastState || {
            clean: true,
            primaryLength: 0,
            partitions: [],
            indexes: []
        };
        this.save({ ...base, clean: false });
    }
}

export default StartupState;
