import expect from 'expect.js';
import fs from 'fs-extra';
import fsSync from 'fs';
import Index, { ReadOnly as ReadOnlyIndex, Entry as IndexEntry } from '../src/Index.js';
import FileHandlePool from '../src/FileHandlePool.js';
import SyncScheduler from '../src/SyncScheduler.js';
import { assertValidEntryClass } from '../src/IndexEntry.js';
import { CHUNK_CAPACITY } from '../src/Index/ReadableIndex.js';
import { fileURLToPath } from 'url';

const __dirname = fileURLToPath(new URL('.', import.meta.url));
const dataDirectory = __dirname + 'data';

describe('Index', function() {

    let index, counter = 1, readers = [];

    beforeEach(function() {
        fs.emptyDirSync(dataDirectory);
    });

    afterEach(function() {
        if (index) index.close();
        for (let reader of readers) reader.close();
        readers = [];
        index = null;
    });

    function createIndex(name = 'test.index', options = {}) {
        return new Index(name, Object.assign({ dataDirectory }, options));
    }

    function setupIndexWithEntries(num, indexMapper, options) {
        if (typeof indexMapper === 'object') {
            options = indexMapper;
            indexMapper = null;
        }
        index = createIndex('test' + (counter++) + '.index', options);
        for (let i = 1; i <= num; i++) {
            index.add(new IndexEntry(indexMapper && indexMapper(i) || i, i));
        }
        index.flush();
        return index;
    }

    function entriesOf(index, from = 1, until = index.length) {
        const entries = [];
        for (let position = from; position <= until; position++) {
            entries.push(index.get(position));
        }
        return entries;
    }

    function createReader(name, options) {
        let reader = new ReadOnlyIndex(name, Object.assign({ dataDirectory }, options));
        readers[readers.length] = reader;
        return reader;
    }

    describe('syncScheduler', function() {

        let fsyncCalls, originalFsync;

        beforeEach(function() {
            fsyncCalls = 0;
            originalFsync = fsSync.fsyncSync;
            fsSync.fsyncSync = (fd) => { fsyncCalls++; return originalFsync(fd); };
        });

        afterEach(function() {
            fsSync.fsyncSync = originalFsync;
        });

        it('syncs a new file with its directory and every flush with syncOnFlush', function() {
            index = createIndex('durable.index', { syncScheduler: new SyncScheduler({ syncOnFlush: true }) });
            index.open();
            expect(fsyncCalls).to.be(2);
            index.add(new Index.Entry(1, 0));
            index.flush();
            expect(fsyncCalls).to.be(3);
        });

        it('syncs flushed entries and the directory of a new file with the next run', function() {
            const syncScheduler = new SyncScheduler({ syncInterval: 1000 });
            index = createIndex('durable.index', { syncScheduler });
            index.open();
            index.add(new Index.Entry(1, 0));
            expect(fsyncCalls).to.be(0);
            syncScheduler.run();
            expect(fsyncCalls).to.be(2);
        });

        it('never syncs by default', function() {
            index = setupIndexWithEntries();
            index.close();
            expect(fsyncCalls).to.be(0);
        });
    });

    it('is opened on instantiation', function() {
        index = setupIndexWithEntries();
        expect(index.isOpen()).to.be(true);
    });

    it('can be opened multiple times', function() {
        index = createIndex();
        index.open();
        expect(index.open()).to.be(false);
    });

    it('flushes buffered entries before a pool eviction', function() {
        const fileHandlePool = new FileHandlePool(1);
        index = createIndex('evicted.index', { fileHandlePool, flushDelay: 0 });
        const initialFd = index.getFileHandle();
        const closedFds = [];
        const originalBeforeClose = index.onBeforeClose;
        index.onBeforeClose = (fd) => {
            closedFds.push(fd);
            return originalBeforeClose.call(index, fd);
        };
        index.add(new IndexEntry(1, 11));

        const other = createIndex('other.index', { fileHandlePool, flushDelay: 0 });
        other.add(new IndexEntry(1, 22));

        expect(closedFds).to.eql([initialFd]);
        expect(index.hasFileHandle()).to.be(false);
        expect(index.flush()).to.be(false);
        expect(index.hasFileHandle()).to.be(false);

        const reader = createReader(index.name);
        expect(reader.get(1).position).to.be(11);
        expect(() => index.close()).to.not.throwError();

        other.close();
    });

    it('writes buffered entries on close after the pool evicted its file handle', function() {
        const fileHandlePool = new FileHandlePool(1);
        index = createIndex('evicted.index', { fileHandlePool });
        index.add(new IndexEntry(1, 11));
        const other = createIndex('other.index', { fileHandlePool });
        expect(index.hasFileHandle()).to.be(false);

        let flushed = false;
        index.add(new IndexEntry(2, 22), () => flushed = true);
        index.close();
        other.close();

        expect(flushed).to.be(true);
        const reader = createReader(index.name);
        expect(reader.length).to.be(2);
        expect(reader.get(2).position).to.be(22);
    });

    it('defaults name to ".index"', function() {
        index = new Index({ dataDirectory });
        expect(index.name).to.be('.index');
    });

    it('recovers metadata on reopening', function() {
        index = createIndex('.index', { metadata: { test: 'valueStays' } });
        expect(index.metadata.test).to.be('valueStays');
        index.close();
        index = createIndex(index.name);
        expect(index.metadata.test).to.be('valueStays');
    });

    it('throws on opening an non-index file', function() {
        const indexFile = dataDirectory + '/.index';
        fs.writeFileSync(indexFile, 'foo');
        expect(() => index = new Index(indexFile)).to.throwError(/Invalid file header/);
    });

    it('throws on opening an index file with different version', function() {
        const indexFile = dataDirectory + '/.index';
        fs.writeFileSync(indexFile, 'nesidx00');
        expect(() => index = new Index(indexFile)).to.throwError(/Invalid file version/);
    });

    it('throws on opening an index file with wrong metadata size', function() {
        const indexFile = dataDirectory + '/.index';
        const metadataBuffer = Buffer.allocUnsafe(8 + 4);
        metadataBuffer.write("nesidx01", 0, 8, 'utf8');
        metadataBuffer.writeUInt32BE(0, 8);
        fs.writeFileSync(indexFile, metadataBuffer);

        expect(() => index = new Index(indexFile)).to.throwError(/Invalid metadata size/);
    });

    it('throws on opening an index file with too large metadata size', function() {
        const indexFile = dataDirectory + '/.index';
        const metadataBuffer = Buffer.allocUnsafe(8 + 4 + 3);
        metadataBuffer.write("nesidx01", 0, 8, 'utf8');
        metadataBuffer.writeUInt32BE(255, 8);
        metadataBuffer.write("{}\n", 12, 3, 'utf8');
        fs.writeFileSync(indexFile, metadataBuffer);

        expect(() => index = new Index(indexFile)).to.throwError(/Invalid index file/);
    });

    it('throws on opening an index file with invalid metadata', function() {
        const indexFile = dataDirectory + '/.index';
        const metadataBuffer = Buffer.allocUnsafe(8 + 4 + 3);
        metadataBuffer.write("nesidx01", 0, 8, 'utf8');
        metadataBuffer.writeUInt32BE(255, 8);
        metadataBuffer.write("{x$", 12, 3, 'utf8');
        fs.writeFileSync(indexFile, metadataBuffer);

        expect(() => index = new Index(indexFile)).to.throwError(/Invalid metadata/);
    });

    it('throws on reopening with altered metadata', function() {
        index = createIndex('.index', { metadata: { test: 'valueStays' } });
        expect(() => index = createIndex(index.name, { metadata: { test: 'anotherValue' } })).to.throwError(/Index metadata mismatch/);
    });

    it('truncates on opening with altered file', function() {
        index = setupIndexWithEntries(5);
        index.close();
        fs.appendFileSync(index.fileName, 'foo');
        expect(() => index = createIndex(index.name)).to.not.throwError();
        expect(index.length).to.be(5);
    });

    describe('Entry', function() {

        it('stores data correctly', function() {
            let entry = new IndexEntry(1, 2, 3, 4);
            expect(entry.number).to.be(1);
            expect(entry.position).to.be(2);
            expect(entry.size).to.be(3);
            expect(entry.partition).to.be(4);
        });

        it('correctly validates custom entry classes', function() {
            class CustomEntryClassWithMissingFromBuffer {
                static get size() { return 4; }
            }
            class CustomEntryClassWithMissingToBuffer {
                static get size() { return 4; }
                static fromBuffer(buffer, offset = 0) {}
            }
            class CustomZeroSizeEntryClass {
                static get size() { return 0; }
                static fromBuffer(buffer, offset = 0) {}
                toBuffer(buffer, offset) {}
            }
            function CustomEs5Entry() {}
            CustomEs5Entry.size = 4;
            CustomEs5Entry.fromBuffer = function(buffer, offset) {};
            CustomEs5Entry.prototype.toBuffer = function(buffer, offset) {};
            expect(() => assertValidEntryClass({})).to.throwError(/Invalid index entry class/);
            expect(() => assertValidEntryClass(CustomEntryClassWithMissingFromBuffer)).to.throwError(/Invalid index entry class/);
            expect(() => assertValidEntryClass(CustomEntryClassWithMissingToBuffer)).to.throwError(/Invalid index entry class/);
            expect(() => assertValidEntryClass(CustomZeroSizeEntryClass)).to.throwError(/size must be positive/);
            expect(() => assertValidEntryClass(CustomEs5Entry)).to.not.throwError();
        });

    });

    describe('add', function() {

        it('appends entries sequentially', function() {
            index = setupIndexWithEntries(25);
            index.close();
            index.open();
            let entries = entriesOf(index);
            expect(entries.length).to.be(25);
            for (let i = 1; i <= entries.length; i++) {
                expect(entries[i - 1].number).to.be(i);
            }
        });

        it('appends entries to reopened index correctly', function() {
            index = setupIndexWithEntries(5);
            index.close();
            index.open();
            index.add(new IndexEntry(6, 6));
            let entries = entriesOf(index);
            expect(entries.length).to.be(6);
            for (let i = 1; i <= entries.length; i++) {
                expect(entries[i - 1].number).to.be(i);
            }
        });

        it('calls callback eventually', function(done) {
            index = createIndex('.index', { flushDelay: 1 });
            let position = index.add(new IndexEntry(1, 0), (number) => {
                expect(number).to.be(position);
                done();
            });
        });

        it('flushes automatically when writeBuffer full', function() {
            index = setupIndexWithEntries(5, { writeBufferSize: 5 * IndexEntry.size });
            expect(index.flush()).to.be(false);
        });

        it('throws with invalid entry object', function() {
            index = createIndex();
            expect(() => index.add([1,2,3,4])).to.throwError(/Wrong entry object/);
        });

        it('throws with invalid entry size', function() {
            index = createIndex();
            class Entry extends IndexEntry {
                static get size() {
                    return 20;
                }
            }
            expect(() => index.add(new Entry(1, 0))).to.throwError(/Invalid entry size/);
        });

    });

    describe('get', function() {

        it('returns false on out of bounds position', function() {
            index = setupIndexWithEntries(5);
            index.close();
            index.open();
            expect(index.get(0)).to.be(false);
            expect(index.get(index.length+1)).to.be(false);
        });

        it('returns false on closed index', function() {
            index = setupIndexWithEntries(5);
            index.close();
            expect(index.get(1)).to.be(false);
        });

        it('can read entry from the end', function() {
            setupIndexWithEntries(5);
            index.close();
            index.open();
            let entry = index.get(-1);
            expect(entry.number).to.be(index.length);
        });

        it('can random read entries', function() {
            index = setupIndexWithEntries(10);
            index.close();
            index.open();
            let entry = index.get(5);
            expect(entry.number).to.be(5);
        });

        it('can read entries multiple times', function() {
            index = setupIndexWithEntries(10);
            index.close();
            index.open();
            for (let i = 0; i < 5; i++) {
                let entry = index.get(5);
                expect(entry.number).to.be(5);
            }
        });

    });

    describe('range', function() {

        function numbers(range) {
            return Array.from(range, entry => entry.number);
        }

        it('iterates an arbitrary range of entries forwards and backwards', function() {
            index = setupIndexWithEntries(50);
            index.close();
            index.open();
            expect(numbers(index.range(21, 25))).to.eql([21, 22, 23, 24, 25]);
            expect(numbers(index.range(25, 21))).to.eql([25, 24, 23, 22, 21]);
            expect(numbers(index.range(21, 21))).to.eql([21]);
        });

        it('counts negative positions from the end', function() {
            index = setupIndexWithEntries(50);
            expect(numbers(index.range(-3))).to.eql([48, 49, 50]);
            expect(numbers(index.range(1, -48))).to.eql([1, 2, 3]);
            expect(numbers(index.range()).length).to.be(50);
            expect(numbers(index.all())).to.eql(numbers(index.range(1, 50)));
        });

        it('is empty for out of bounds positions or a closed index', function() {
            index = setupIndexWithEntries(5);
            expect(numbers(index.range(0))).to.eql([]);
            expect(numbers(index.range(6, 8))).to.eql([]);
            expect(numbers(index.range(1, 6))).to.eql([]);
            expect(numbers(index.range('foo'))).to.eql([]);
            index.close();
            expect(numbers(index.range(1, 5))).to.eql([]);
        });

        it('skips ahead to a sequence number given to next()', function() {
            index = setupIndexWithEntries(100, i => 2 * i);
            const forwards = index.range();
            forwards.next();
            expect(forwards.next(75).value.number).to.be(76);
            expect(forwards.next(78).value.number).to.be(78);
            expect(forwards.next(201).done).to.be(true);

            const backwards = index.range(-1, 1);
            backwards.next();
            expect(backwards.next(41).value.number).to.be(40);
            expect(backwards.next(1).done).to.be(true);
        });

    });

    describe('lastEntry', function() {

        it('returns the last entry', function() {
            index = setupIndexWithEntries(5);
            expect(index.lastEntry.number).to.be(5);
        });

        it('returns false on empty index', function() {
            index = setupIndexWithEntries(0);
            expect(index.lastEntry).to.be(false);
        });

    });

    describe('find', function() {

        it('returns 0 on an empty index', function() {
            index = createIndex();
            expect(index.find(1)).to.be(0);
        });

        it('returns 0 if no entry is lower or equal searched number', function() {
            index = setupIndexWithEntries(5, i => 5 + i);
            expect(index.find(index.length)).to.be(0);
        });

        it('returns last entry if all entries are lower searched number', function() {
            index = setupIndexWithEntries(5);
            expect(index.find(index.length+1)).to.be(index.length);
        });

        it('returns 0 if all entries are lower searched number with min=true', function() {
            index = setupIndexWithEntries(5);
            expect(index.find(index.length+1, true)).to.be(0);
        });

        it('returns the entry number on exact match', function() {
            index = setupIndexWithEntries(5);
            for (let i = 1; i <= 5; i++) {
                expect(index.find(i)).to.be(i);
            }
        });

        it('returns the highest entry number lower than the searched number', function() {
            index = setupIndexWithEntries(50, i => 2*i);
            expect(index.find(25)).to.be(12);
        });

        it('returns the lowest entry number higher than the searched number with min=true', function() {
            index = setupIndexWithEntries(50, i => 2*i);
            expect(index.find(25, true)).to.be(13);
        });

    });

    describe('truncate', function() {

        it('truncates after the given index position', function() {
            index = setupIndexWithEntries(5);
            index.close();
            index.open();

            index.truncate(2);
            expect(index.length).to.be(2);

            index.close();
            index.open();
            expect(index.length).to.be(2);
        });

        it('correctly truncates after unflushed entries', function() {
            index = setupIndexWithEntries(5);

            index.truncate(2);
            expect(index.length).to.be(2);

            index.close();
            index.open();
            expect(index.length).to.be(2);
        });

        it('does not truncate closed index', function() {
            index = setupIndexWithEntries(5);
            index.close();

            index.truncate(2);

            index.open();
            expect(index.length).to.be(5);
        });

        it('does nothing if truncating after index length', function() {
            index = setupIndexWithEntries(5);
            index.close();
            index.open();

            index.truncate(6);
            expect(index.length).to.be(5);

            index.close();
            index.open();
            expect(index.length).to.be(5);
        });

        it('truncates whole index if given negative position', function() {
            index = setupIndexWithEntries(5);
            index.close();
            index.open();

            index.truncate(-5);
            expect(index.length).to.be(0);

            index.close();
            index.open();
            expect(index.length).to.be(0);
        });

    });

    describe('destroy', function(){

        it('completely deletes the file', function(){
            index = setupIndexWithEntries(5);
            const fileName = index.fileName;
            index.destroy();
            expect(fs.existsSync(fileName)).to.be(false);
        });

    });

    describe('flush', function(){

        it('returns false on a closed index', function(){
            index = setupIndexWithEntries(1);
            index.close();
            expect(index.flush()).to.be(false);
        });

        it('returns false if nothing to flush', function(){
            index = setupIndexWithEntries(1);
            index.flush();
            expect(index.flush()).to.be(false);
        });

    });

    describe('chunked entries', function() {

        const entryCount = 2 * CHUNK_CAPACITY + 10;

        it('reads entries across chunk boundaries after reopening', function() {
            index = setupIndexWithEntries(entryCount, i => 3 * i);
            index.close();
            index.open();
            expect(index.get(CHUNK_CAPACITY + 1).number).to.be(3 * (CHUNK_CAPACITY + 1));
            expect(index.get(1).number).to.be(3);
            const entries = entriesOf(index, CHUNK_CAPACITY - 1, CHUNK_CAPACITY + 2);
            expect(entries.map(entry => entry.number)).to.eql([CHUNK_CAPACITY - 1, CHUNK_CAPACITY, CHUNK_CAPACITY + 1, CHUNK_CAPACITY + 2].map(i => 3 * i));
            expect(entriesOf(index).length).to.be(entryCount);
            expect(index.lastEntry.number).to.be(3 * entryCount);
        });

        it('finds numbers across chunk boundaries without loading the whole index first', function() {
            index = setupIndexWithEntries(entryCount, i => 2 * i);
            index.close();
            index.open();
            expect(index.find(2 * (CHUNK_CAPACITY + 5))).to.be(CHUNK_CAPACITY + 5);
            expect(index.find(2 * (CHUNK_CAPACITY + 5) + 1)).to.be(CHUNK_CAPACITY + 5);
            expect(index.find(2 * (CHUNK_CAPACITY + 5) + 1, true)).to.be(CHUNK_CAPACITY + 6);
        });

        it('appends to a reopened index whose last chunk was not read yet', function() {
            index = setupIndexWithEntries(CHUNK_CAPACITY + 3);
            index.close();
            index.open();
            index.add(new IndexEntry(CHUNK_CAPACITY + 4, 0));
            index.flush();
            index.close();
            index.open();
            expect(index.length).to.be(CHUNK_CAPACITY + 4);
            expect(entriesOf(index, CHUNK_CAPACITY + 2).map(entry => entry.number)).to.eql([CHUNK_CAPACITY + 2, CHUNK_CAPACITY + 3, CHUNK_CAPACITY + 4]);
        });

        it('flushes unwritten entries that span several chunks', function() {
            index = createIndex('large-buffer.index', { writeBufferSize: 4 * CHUNK_CAPACITY * IndexEntry.size });
            const count = CHUNK_CAPACITY + 100;
            for (let i = 1; i <= count; i++) {
                index.add(new IndexEntry(i, i));
            }
            expect(index.flush()).to.be(true);
            index.close();
            index.open();
            expect(index.length).to.be(count);
            expect(entriesOf(index, CHUNK_CAPACITY - 1, CHUNK_CAPACITY + 2).map(entry => entry.position)).to.eql([CHUNK_CAPACITY - 1, CHUNK_CAPACITY, CHUNK_CAPACITY + 1, CHUNK_CAPACITY + 2]);
            expect(index.lastEntry.number).to.be(count);
        });

        it('truncates across chunk boundaries and appends again', function() {
            index = setupIndexWithEntries(entryCount);
            index.truncate(CHUNK_CAPACITY - 2);
            expect(index.length).to.be(CHUNK_CAPACITY - 2);
            expect(index.lastEntry.number).to.be(CHUNK_CAPACITY - 2);
            for (let i = CHUNK_CAPACITY - 1; i <= CHUNK_CAPACITY + 2; i++) {
                index.add(new IndexEntry(i, 100 + i));
            }
            index.flush();
            expect(index.get(CHUNK_CAPACITY + 1).position).to.be(100 + CHUNK_CAPACITY + 1);
            index.close();
            index.open();
            expect(index.length).to.be(CHUNK_CAPACITY + 2);
            expect(index.get(CHUNK_CAPACITY + 1).position).to.be(100 + CHUNK_CAPACITY + 1);
        });

        it('supports custom entry classes with a different layout', function() {
            class WideEntry extends IndexEntry {
                static get size() {
                    return 20;
                }
                static fromBuffer(buffer, offset = 0) {
                    return new this(buffer.readUInt32LE(offset + 4), buffer.readUInt32LE(offset + 8));
                }
                toBuffer(buffer, offset) {
                    buffer.writeUInt32LE(0xffffffff, offset);
                    buffer.writeUInt32LE(this.number, offset + 4);
                    buffer.writeUInt32LE(this.position, offset + 8);
                    buffer.fill(0, offset + 12, offset + 20);
                    return WideEntry.size;
                }
            }
            index = createIndex('wide.index', { EntryClass: WideEntry });
            for (let i = 1; i <= CHUNK_CAPACITY + 5; i++) {
                index.add(new WideEntry(2 * i, i));
            }
            index.flush();
            index.close();
            index.open();
            expect(index.get(CHUNK_CAPACITY + 2)).to.be.a(WideEntry);
            expect(index.get(CHUNK_CAPACITY + 2).position).to.be(CHUNK_CAPACITY + 2);
            expect(index.find(2 * (CHUNK_CAPACITY + 3))).to.be(CHUNK_CAPACITY + 3);
        });

        it('lets a reader see entries appended into an already read chunk', function(done) {
            index = setupIndexWithEntries(5);
            const reader = createReader(index.name);
            expect(reader.get(5).number).to.be(5);
            reader.on('append', (prevLength, newLength) => {
                expect(entriesOf(reader, prevLength + 1, newLength).map(entry => entry.number)).to.eql([6, 7]);
                done();
            });
            index.add(new IndexEntry(6, 6));
            index.add(new IndexEntry(7, 7));
            index.flush();
        });
    });

    describe('ReadOnly', function(){

        it('can be created without explicit name', function(){
            expect(() => {
                index = createIndex('.index');
                let reader = new ReadOnlyIndex({ dataDirectory });
                reader.close();
            }).to.not.throwError();
        });

        it('can be opened and closed multiple times', function(){
            index = createIndex('.index');
            let reader = new ReadOnlyIndex({ dataDirectory });
            expect(reader.open()).to.be(false);
            reader.close();
            reader.close();
        });

        it('throws when opening an empty file', function(){
            index = createIndex('.index');
            index.close();
            fs.truncateSync(index.fileName, 0);
            expect(() => createReader(index.name)).to.throwError(/empty/);
        });

        it('allows multiple readers for a single index', function(){
            index = setupIndexWithEntries(5);

            let reader1 = createReader(index.name);
            expect(reader1.isOpen()).to.be(true);
            expect(reader1.length).to.be(index.length);
            expect(reader1.lastEntry.number).to.be(index.lastEntry.number);

            let reader2 = createReader(index.name);
            expect(reader2.isOpen()).to.be(true);
            expect(reader2.length).to.be(index.length);
            expect(reader2.lastEntry.number).to.be(index.lastEntry.number);
        });

        it('updates when writer flushes', function(done){
            index = setupIndexWithEntries(5);
            let reader1 = createReader(index.name);

            reader1.on('append', (prev, next) => {
                expect(prev).to.be(5);
                expect(next).to.be(6);
                expect(reader1.get(next).number).to.be(index.get(next).number);
                done();
            });

            index.add(new IndexEntry(6, 6));
            index.flush();
            fs.fdatasync(index.getFileHandle());
        });

        it('updates when writer truncates', function(done){
            index = setupIndexWithEntries(5);
            let reader1 = createReader(index.name);

            reader1.on('truncate', (prev, next) => {
                expect(prev).to.be(5);
                expect(next).to.be(0);
                expect(reader1.length).to.be(0);
                done();
            });

            index.truncate(0);
            fs.fdatasync(index.getFileHandle());
        });

        it('closes when file renamed', function(done){
            index = setupIndexWithEntries(5);
            index.close();
            let reader = createReader(index.name);
            expect(reader.isOpen()).to.be(true);

            fs.rename(reader.fileName, reader.fileName + '2', () => {
                setTimeout(() => {
                    expect(reader.isOpen()).to.be(false);
                    done();
                }, 1);
            });
        });

        it('does not trigger handler when index closed', function(done){
            index = setupIndexWithEntries(5);

            let reader = createReader(index.name);
            reader.on('truncate', () => expect(this).to.be(false));

            index.truncate(0);
            reader.close();
            fs.fdatasync(index.getFileHandle(), () => done());
        });
    });
});
