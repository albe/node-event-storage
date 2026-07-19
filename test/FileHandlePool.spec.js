import expect from 'expect.js';
import fs from 'fs-extra';
import FileHandlePool from '../src/FileHandlePool.js';
import { fileURLToPath } from 'url';

const __dirname = fileURLToPath(new URL('.', import.meta.url));
const dataDirectory = __dirname + 'data';

describe('FileHandlePool', function() {

    beforeEach(function() {
        fs.emptyDirSync(dataDirectory);
    });

    function createTarget(name, overrides = {}) {
        return Object.assign({
            fileName: dataDirectory + '/' + name,
            fileMode: 'a+'
        }, overrides);
    }

    it('updates the LRU order when touching an open target', function() {
        const pool = new FileHandlePool(2);
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

        pool.evict(target1, false);
        pool.evict(target3, false);
    });

    it('returns false when evicting an unknown target and ignores missing touches', function() {
        const pool = new FileHandlePool(1);
        const target = createTarget('missing');

        expect(() => pool.touch(target)).to.not.throwError();
        expect(pool.evict(target)).to.be(false);
    });

    it('passes evicted=false to the before-close hook on explicit close', function() {
        const pool = new FileHandlePool(1);
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

});
