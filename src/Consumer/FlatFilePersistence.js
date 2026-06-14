import fs from 'fs';
import path from 'path';
import { ensureDirectory, safeUnlink, scanForFiles } from '../utils/fsUtil.js';
import { normalizeConsumerStateArgs } from '../utils/apiHelpers.js';
import ConsumerPersistence from './ConsumerPersistence.js';

const escapeForRegExp = (value) => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

/**
 * Default file-based persistence for Consumer state and position.
 */
class FlatFilePersistence extends ConsumerPersistence {
    /**
     * @param {string} indexDirectory
     * @param {string} storageFile
     * @param {string} indexName
     * @param {string} identifier
     */
    constructor(indexDirectory, storageFile, indexName, identifier) {
        super();
        this.consumerDirectory = path.join(indexDirectory, 'consumers');
        this.fileName = path.join(this.consumerDirectory, storageFile + '.' + indexName + '.' + identifier);
        ensureDirectory(this.consumerDirectory);
    }

    /**
     * Finish file-system prep that is safe to do asynchronously before the consumer starts processing.
     * @returns {Promise<void>}
     */
    open() {
        return new Promise((resolve, reject) => {
            this.cleanUpFailedWrites((error) => {
                if (error) {
                    reject(error);
                    return;
                }
                resolve();
            });
        });
    }

    /**
     * Iterate over all files in this consumer directory and unlink tmp files left behind by failed writes.
     * @param {function(Error=): void} callback
     */
    cleanUpFailedWrites(callback) {
        const consumerNamePrefix = path.basename(this.fileName) + '.';
        const matchedFiles = [];
        const filePattern = new RegExp(`^${escapeForRegExp(consumerNamePrefix)}.+$`);

        scanForFiles(this.consumerDirectory, filePattern, matchedFiles.push.bind(matchedFiles), (error) => {
            if (error) {
                callback(error);
                return;
            }

            try {
                for (const file of matchedFiles) {
                    safeUnlink(path.join(this.consumerDirectory, file));
                }
                callback(null);
            } catch (unlinkError) {
                callback(unlinkError);
            }
        });
    }

    /**
     * @param {object} initialState
     * @param {number} startFrom
     * @returns {Promise<{ position: number, state: object }>}
     */
    async load(initialState = {}, startFrom = 0) {
        ({ initialState, startFrom } = normalizeConsumerStateArgs(initialState, startFrom));
        try {
            const consumerData = await fs.promises.readFile(this.fileName);
            return {
                position: consumerData.readInt32LE(0),
                state: JSON.parse(consumerData.toString('utf8', 4))
            };
        } catch (e) {
            return {
                position: startFrom,
                state: initialState
            };
        }
    }

    /**
     * @param {number} position
     * @param {object} state
     * @param {string} consumerName
     * @returns {Promise<string>}
     */
    async persist(position, state, consumerName = '') {
        const consumerState = JSON.stringify(state);
        const consumerData = Buffer.allocUnsafe(4 + consumerState.length);
        consumerData.writeInt32LE(position, 0);
        consumerData.write(consumerState, 4, consumerState.length, 'utf-8');

        const tmpFile = this.fileName + '.' + position;
        try {
            await fs.promises.access(tmpFile, fs.constants.F_OK);
            throw new Error(`Trying to update consumer ${consumerName} concurrently. Keep each single consumer within a single process.`);
        } catch (e) {
            if (e.code !== 'ENOENT') {
                throw e;
            }
        }

        try {
            await fs.promises.writeFile(tmpFile, consumerData);
            // Atomic rename prevents corrupting the last valid state file on partial writes.
            await fs.promises.rename(tmpFile, this.fileName);
            return consumerState;
        } catch (e) {
            /* c8 ignore next */
            safeUnlink(tmpFile);
            throw e;
        }
    }
}

export default FlatFilePersistence;