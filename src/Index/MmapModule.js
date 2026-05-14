import { createRequire } from 'module';

const require = createRequire(import.meta.url);
const supportedMmapPackages = ['@riaskov/mmap-io', 'mmap-io', '@fayzanx/mmap-io'];

let mmapModule = null;
let mmapPackageName = null;

function loadMmapModule() {
    if (mmapModule) {
        return mmapModule;
    }
    for (const packageName of supportedMmapPackages) {
        try {
            mmapModule = require(packageName);
            mmapPackageName = packageName;
            return mmapModule;
        } catch (e) {
            // Try next package.
        }
    }
    throw new Error(`No compatible mmap-io implementation installed. Tried: ${supportedMmapPackages.join(', ')}`);
}

function getMmapPackageName() {
    loadMmapModule();
    return mmapPackageName;
}

/**
 * Some mmap modules do not expose an explicit unmap API.
 * We call it only when available and otherwise rely on GC to release the buffer.
 *
 * @param {object} mmap
 * @param {Buffer|null} buffer
 */
function unmapBuffer(mmap, buffer) {
    if (!buffer) {
        return;
    }
    if (typeof mmap.unmap === 'function') {
        mmap.unmap(buffer);
    }
}

export { loadMmapModule, getMmapPackageName, unmapBuffer };
