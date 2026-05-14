import expect from 'expect.js';
import fs from 'fs-extra';
import { fileURLToPath } from 'url';
import MmapWritableIndex from '../src/Index/MmapWritableIndex.js';
import { Entry as IndexEntry } from '../src/Index.js';

const __dirname = fileURLToPath(new URL('.', import.meta.url));
const dataDirectory = __dirname + 'data';

describe('MmapIndex', function() {
    let index = null;

    beforeEach(function() {
        fs.emptyDirSync(dataDirectory);
    });

    afterEach(function() {
        if (index) {
            index.close();
            index = null;
        }
    });

    it('fills and reuses cache while yielding range entries', function() {
        index = new MmapWritableIndex('test.index', { dataDirectory });
        for (let i = 1; i <= 5; i++) {
            index.add(new IndexEntry(i, i));
        }
        index.flush();

        const firstPass = Array.from(index.range(2, 4));
        expect(firstPass).to.have.length(3);
        expect(index.get(2)).to.be(firstPass[0]);
        expect(index.get(3)).to.be(firstPass[1]);
        expect(index.get(4)).to.be(firstPass[2]);

        const secondPass = Array.from(index.range(2, 4));
        expect(secondPass[0]).to.be(firstPass[0]);
        expect(secondPass[1]).to.be(firstPass[1]);
        expect(secondPass[2]).to.be(firstPass[2]);
    });
});
