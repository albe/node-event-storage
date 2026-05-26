import expect from 'expect.js';
import fs from 'fs-extra';
import { once } from 'events';
import { fileURLToPath } from 'url';
import MmapIndex, { ReadOnly as MmapReadOnlyIndex, Entry as IndexEntry } from '../src/MmapIndex.js';

const __dirname = fileURLToPath(new URL('.', import.meta.url));
const dataDirectory = __dirname + 'data';

describe('MmapIndex', function () {

    let index, readers = [];

    beforeEach(function () {
        fs.emptyDirSync(dataDirectory);
    });

    afterEach(function () {
        if (index) index.close();
        for (const reader of readers) reader.close();
        readers = [];
        index = null;
    });

    function createIndex(name = '.mmap.index', options = {}) {
        return new MmapIndex(name, Object.assign({ dataDirectory }, options));
    }

    function createReader(name = '.mmap.index', options = {}) {
        const reader = new MmapReadOnlyIndex(name, Object.assign({ dataDirectory }, options));
        readers.push(reader);
        return reader;
    }

    it('is opened on instantiation', function () {
        index = createIndex();
        expect(index.isOpen()).to.be(true);
    });

    it('can be opened multiple times without effect', function () {
        index = createIndex();
        expect(index.open()).to.be(false);
    });

    it('defaults name to ".index"', function () {
        index = new MmapIndex({ dataDirectory });
        expect(index.name).to.be('.index');
    });

    it('starts with length 0', function () {
        index = createIndex();
        expect(index.length).to.be(0);
    });

    it('appends an entry and reports the correct length', function () {
        index = createIndex();
        index.add(new IndexEntry(1, 100));
        expect(index.length).to.be(1);
    });

    it('reads back a single appended entry', function () {
        index = createIndex();
        index.add(new IndexEntry(1, 42, 16, 0));
        index.flush();
        const entry = index.get(1);
        expect(entry.number).to.be(1);
        expect(entry.position).to.be(42);
        expect(entry.size).to.be(16);
    });

    it('returns false for out-of-range get()', function () {
        index = createIndex();
        expect(index.get(0)).to.be(false);
        expect(index.get(1)).to.be(false);
    });

    it('returns lastEntry', function () {
        index = createIndex();
        expect(index.lastEntry).to.be(false);
        index.add(new IndexEntry(3, 200));
        index.flush();
        expect(index.lastEntry.number).to.be(3);
    });

    it('range() returns entries in the given 1-based window', function () {
        index = createIndex();
        for (let i = 1; i <= 5; i++) {
            index.add(new IndexEntry(i, i * 10));
        }
        index.flush();
        const entries = index.range(2, 4);
        expect(entries.length).to.be(3);
        expect(entries[0].number).to.be(2);
        expect(entries[2].number).to.be(4);
    });

    it('all() returns all entries', function () {
        index = createIndex();
        for (let i = 1; i <= 3; i++) {
            index.add(new IndexEntry(i, i));
        }
        index.flush();
        const all = index.all();
        expect(all.length).to.be(3);
    });

    it('find() locates the entry by sequence number', function () {
        index = createIndex();
        for (let i = 1; i <= 5; i++) {
            index.add(new IndexEntry(i, i * 100));
        }
        index.flush();
        expect(index.find(3)).to.be(3);
        expect(index.find(6)).to.be(5);  // past end → last high
    });

    it('throws on out-of-order add()', function () {
        index = createIndex();
        index.add(new IndexEntry(5, 500));
        expect(() => index.add(new IndexEntry(3, 300))).to.throwError(/Consistency error/);
    });

    it('persists entries across close and reopen', function () {
        index = createIndex('persist.index');
        for (let i = 1; i <= 4; i++) {
            index.add(new IndexEntry(i, i * 10));
        }
        index.flush();
        index.close();
        index = null;

        index = createIndex('persist.index');
        expect(index.length).to.be(4);
        expect(index.get(4).number).to.be(4);
    });

    it('truncate() removes trailing entries', function () {
        index = createIndex();
        for (let i = 1; i <= 5; i++) {
            index.add(new IndexEntry(i, i));
        }
        index.flush();
        index.truncate(3);
        expect(index.length).to.be(3);
        expect(index.lastEntry.number).to.be(3);
    });

    it('preserves user metadata across reopen', function () {
        index = createIndex('meta.index', { metadata: { stream: 'orders' } });
        expect(index.metadata.stream).to.be('orders');
        index.close();
        index = null;

        index = createIndex('meta.index');
        expect(index.metadata.stream).to.be('orders');
    });

    it('throws on metadata mismatch', function () {
        createIndex('meta.index', { metadata: { stream: 'a' } }).close();
        expect(() => createIndex('meta.index', { metadata: { stream: 'b' } })).to.throwError(/metadata mismatch/);
    });

    it('MmapReadOnlyIndex emits append when writer flushes new entries', function (done) {
        index = createIndex('watch.index');
        index.flush();

        const reader = createReader('watch.index');
        once(reader, 'append').then(([prevLen, newLen]) => {
            expect(prevLen).to.be(0);
            expect(newLen).to.be(2);
            expect(reader.get(1).number).to.be(1);
            expect(reader.get(2).number).to.be(2);
            done();
        }).catch(done);

        index.add(new IndexEntry(1, 10));
        index.add(new IndexEntry(2, 20));
        index.flush();
    });

    it('MmapReadOnlyIndex emits truncate when writer truncates', function (done) {
        index = createIndex('trunc.index');
        for (let i = 1; i <= 3; i++) {
            index.add(new IndexEntry(i, i));
        }
        index.flush();

        const reader = createReader('trunc.index');
        expect(reader.length).to.be(3);

        once(reader, 'truncate').then(([prevLen, newLen]) => {
            expect(prevLen).to.be(3);
            expect(newLen).to.be(1);
            done();
        }).catch(done);

        index.truncate(1);
        index.flush();
    });
});
