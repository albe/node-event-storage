const expect = require('expect.js');
const fs = require('fs-extra');
const Index = require('../src/Index');

describe('Index', function() {

    let index, counter = 1, readers = [];

    beforeEach(function() {
        fs.emptyDirSync('test/data');
    });

    afterEach(function() {
        if (index) index.close();
        for (let reader of readers) reader.close();
        readers = [];
        index = undefined;
    });

    function setupIndexWithEntries(num, indexMapper, options) {
        if (typeof indexMapper === 'object') {
            options = indexMapper;
            indexMapper = undefined;
        }
        index = new Index('test' + (counter++) + '.index', Object.assign({ dataDirectory: 'test/data' }, options));
        for (let i = 1; i <= num; i++) {
            index.add(new Index.Entry(indexMapper && indexMapper(i) || i, i));
        }
        index.flush();
        return index;
    }

    function createReader(name, options) {
        let reader = new Index.ReadOnly(name, Object.assign({ dataDirectory: 'test/data' }, options));
        readers[readers.length] = reader;
        return reader;
    }

    it('is opened on instanciation', function() {
        index = setupIndexWithEntries();
        expect(index.isOpen()).to.be(true);
    });

    it('defaults name to ".index"', function() {
        index = new Index({ dataDirectory: 'test/data' });
        expect(index.name).to.be('.index');
    });

    it('recovers metadata on reopening', function() {
        index = new Index('test/data/.index', { metadata: { test: 'valueStays' } });
        expect(index.metadata.test).to.be('valueStays');
        index.close();
        index = new Index('test/data/.index');
        expect(index.metadata.test).to.be('valueStays');
    });

    it('throws on opening an non-index file', function() {
        fs.writeFileSync('test/data/.index', 'foo');
        expect(() => index = new Index('test/data/.index')).to.throwError(/Invalid file header/);
    });

    it('throws on opening an index file with different version', function() {
        fs.writeFileSync('test/data/.index', 'nesidx00');
        expect(() => index = new Index('test/data/.index')).to.throwError(/Invalid file version/);
    });

    it('throws on opening an index file with wrong metadata size', function() {
        const metadataBuffer = Buffer.allocUnsafe(8 + 4);
        metadataBuffer.write("nesidx01", 0, 8, 'utf8');
        metadataBuffer.writeUInt32BE(0, 8);
        fs.writeFileSync('test/data/.index', metadataBuffer);

        expect(() => index = new Index('test/data/.index')).to.throwError(/Invalid metadata size/);
    });

    it('throws on opening an index file with too large metadata size', function() {
        const metadataBuffer = Buffer.allocUnsafe(8 + 4 + 3);
        metadataBuffer.write("nesidx01", 0, 8, 'utf8');
        metadataBuffer.writeUInt32BE(255, 8);
        metadataBuffer.write("{}\n", 12, 3, 'utf8');
        fs.writeFileSync('test/data/.index', metadataBuffer);

        expect(() => index = new Index('test/data/.index')).to.throwError(/Invalid index file/);
    });

    it('throws on opening an index file with invalid metadata', function() {
        const metadataBuffer = Buffer.allocUnsafe(8 + 4 + 3);
        metadataBuffer.write("nesidx01", 0, 8, 'utf8');
        metadataBuffer.writeUInt32BE(255, 8);
        metadataBuffer.write("{x$", 12, 3, 'utf8');
        fs.writeFileSync('test/data/.index', metadataBuffer);

        expect(() => index = new Index('test/data/.index')).to.throwError(/Invalid metadata/);
    });

    it('throws on reopening with altered metadata', function() {
        index = new Index('test/data/.index', { metadata: { test: 'valueStays' } });
        expect(() => index = new Index('test/data/.index', { metadata: { test: 'anotherValue' } })).to.throwError(/Index metadata mismatch/);
    });

    it('throws on opening with altered file', function() {
        index = setupIndexWithEntries(5);
        index.close();
        fs.appendFileSync('test/data/test.index', 'foo');
        expect(() => index = new Index('test/data/test.index')).to.throwError(/Index file is corrupt/);
    });

    describe('Entry', function() {

        it('stores data correctly', function() {
            let entry = new Index.Entry(1, 2, 3, 4);
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
            expect(() => Index.Entry.assertValidEntryClass({})).to.throwError(/Invalid index entry class/);
            expect(() => Index.Entry.assertValidEntryClass(CustomEntryClassWithMissingFromBuffer)).to.throwError(/Invalid index entry class/);
            expect(() => Index.Entry.assertValidEntryClass(CustomEntryClassWithMissingToBuffer)).to.throwError(/Invalid index entry class/);
            expect(() => Index.Entry.assertValidEntryClass(CustomZeroSizeEntryClass)).to.throwError(/size must be positive/);
            expect(() => Index.Entry.assertValidEntryClass(CustomEs5Entry)).to.not.throwError();
        });

    });

    describe('add', function() {

        it('appends entries sequentially', function() {
            index = setupIndexWithEntries(25);
            index.close();
            index.open();
            let entries = index.all();
            expect(entries.length).to.be(25);
            for (let i = 1; i <= entries.length; i++) {
                expect(entries[i - 1].number).to.be(i);
            }
        });

        it('appends entries to reopened index correctly', function() {
            index = setupIndexWithEntries(5);
            index.close();
            index.open();
            index.add(new Index.Entry(6, 6));
            let entries = index.all();
            expect(entries.length).to.be(6);
            for (let i = 1; i <= entries.length; i++) {
                expect(entries[i - 1].number).to.be(i);
            }
        });

        it('calls callback eventually', function(done) {
            index = new Index('test/data/.index', { flushDelay: 1 });
            let position = index.add(new Index.Entry(1, 0), (number) => {
                expect(number).to.be(position);
                done();
            });
        });

        it('flushes automatically when writeBuffer full', function() {
            index = setupIndexWithEntries(5, { writeBufferSize: 5 * Index.Entry.size });
            expect(index.flush()).to.be(false);
        });

        it('throws with invalid entry object', function() {
            index = new Index('test/data/.index');
            expect(() => index.add([1,2,3,4])).to.throwError(/Wrong entry object/);
        });

        it('throws with invalid entry size', function() {
            index = new Index('test/data/.index');
            class Entry extends Index.Entry {
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

        it('returns false on out of bounds range position', function() {
            index = setupIndexWithEntries(50);
            index.close();
            index.open();
            expect(index.range(0)).to.be(false);
            expect(index.range(51, 55)).to.be(false);
            expect(index.range(1, 51)).to.be(false);
            expect(index.range(15, 10)).to.be(false);
        });

        it('returns false on closed index', function() {
            index = setupIndexWithEntries(5);
            index.close();
            expect(index.range(1)).to.be(false);
            expect(index.range(1,5)).to.be(false);
        });

        it('can read an arbitrary range of entries', function() {
            index = setupIndexWithEntries(50);
            index.close();
            index.open();
            let entries = index.range(21, 37);
            for (let i = 0; i < entries.length; i++) {
                expect(entries[i].number).to.be(21 + i);
            }
        });

        it('can read a range of entries from the end', function() {
            index = setupIndexWithEntries(50);
            index.close();
            index.open();
            let entries = index.range(-15);
            expect(entries.length).to.be(15);
            for (let i = 0; i < entries.length; i++) {
                expect(entries[i].number).to.be(36 + i);
            }
        });

        it('can read a range of entries until a distance from the end', function() {
            index = setupIndexWithEntries(50);
            index.close();
            index.open();
            let entries = index.range(1, -15);
            expect(entries.length).to.be(36);   // 36 because end is inclusive
            for (let i = 0; i < entries.length; i++) {
                expect(entries[i].number).to.be(1 + i);
            }
        });

        it('can read a single item range of entries', function() {
            index = setupIndexWithEntries(50);
            index.close();
            index.open();
            let entries = index.range(21, 21);
            expect(entries.length).to.be(1);
            expect(entries[0].number).to.be(21);
        });

        it('returns false with a non-numeric range', function() {
            index = setupIndexWithEntries(5);
            index.close();
            index.open();
            let entries = index.range('foo');
            expect(entries).to.be(false);
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

    describe('validRange', function(){

        it('returns false for out of range from positions', function(){
            index = setupIndexWithEntries(5);
            expect(index.validRange(0, 1)).to.be(false);
            expect(index.validRange(-1, 1)).to.be(false);
            expect(index.validRange(index.length + 1, index.length + 2)).to.be(false);
        });

        it('returns false when from greater until', function(){
            index = setupIndexWithEntries(5);
            expect(index.validRange(2, 1)).to.be(false);
            expect(index.validRange(1, 0)).to.be(false);
        });

        it('returns false for out of range until positions', function(){
            index = setupIndexWithEntries(5);
            expect(index.validRange(1, -1)).to.be(false);
            expect(index.validRange(1, index.length +1)).to.be(false);
        });

        it('returns true for valid range positions', function(){
            index = setupIndexWithEntries(5);
            expect(index.validRange(1, 1)).to.be(true);
            expect(index.validRange(1, index.length)).to.be(true);
            expect(index.validRange(index.length, index.length)).to.be(true);
        });

    });

    describe('destroy', function(){

        it('completely deletes the file', function(){
            index = setupIndexWithEntries(5);
            index.destroy();
            expect(fs.existsSync('test/data/.index')).to.be(false);
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

    describe('concurrency', function(){

        it('allows only a single writer for a index', function(){
            // t.b.d.
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

        it('updates reader when writer flushes', function(done){
            index = setupIndexWithEntries(5);
            let reader1 = createReader(index.name);

            reader1.on('append', (prev, next) => {
                expect(prev).to.be(5);
                expect(next).to.be(6);
                expect(reader1.get(next).number).to.be(index.get(next).number);
                done();
            });

            index.add(new Index.Entry(6, 6));
            index.flush();
            fs.fdatasync(index.fd);
        });

        it('updates reader when writer truncates', function(done){
            index = setupIndexWithEntries(5);
            let reader1 = createReader(index.name);

            reader1.on('truncate', (prev, next) => {
                expect(prev).to.be(5);
                expect(next).to.be(0);
                expect(reader1.length).to.be(0);
                done();
            });

            index.truncate(0);
            fs.fdatasync(index.fd);
        });
    });
});
