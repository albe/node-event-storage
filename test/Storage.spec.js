const expect = require('expect.js');
const fs = require('fs-extra');
const path = require('path');
const Storage = require('../src/Storage');
const zlib = require('zlib');
//const lz4 = require('lz4');

const dataDirectory = __dirname + '/data';

describe('Storage', function() {

    /**
     * @var {WritableStorage}
     */
    let storage;
    let refs = [];

    function createStorage(options = {}) {
        const newStorage = new Storage(Object.assign({ dataDirectory }, options));
        refs.push(newStorage);
        return newStorage;
    }

    function createReader(options = {}) {
        const newStorage = new Storage.ReadOnly(Object.assign({ dataDirectory }, options));
        refs.push(newStorage);
        return newStorage;
    }

    beforeEach(function () {
        try {
            fs.emptyDirSync(dataDirectory);
        } catch (e) {
        }
    });

    afterEach(function () {
        refs.forEach(ref => ref.close());
        refs = [];
        if (storage) storage.close();
        storage = null;
    });

    it('creates the storage directory if it does not exist', function() {
        fs.removeSync(dataDirectory);
        storage = createStorage();
        expect(fs.existsSync(dataDirectory)).to.be(true);
    });

    it('can be opened multiple times', function(){
        storage = createStorage();
        storage.open();
        expect(() => {
            storage.open();
        }).to.not.throwError();
    });

    describe('write', function() {

        it('writes objects', function(done) {
            storage = createStorage();
            storage.open();

            storage.write({ foo: 'bar' }, done);
        });

        it('writes documents sequentially', function() {
            storage = createStorage();
            storage.open();

            for (let i = 1; i <= 10; i++) {
                expect(storage.write({ foo: 'bar' })).to.be(i);
            }
            expect(storage.index.length).to.be(10);
        });

        it('can write durable', function(done) {
            storage = createStorage({ maxWriteBufferDocuments: 1, syncOnFlush: true });
            storage.open();

            storage.write({ foo: 'bar' }, () => {
                let fileContent = fs.readFileSync('test/data/storage', 'utf8');
                expect(fileContent).to.contain(JSON.stringify({ foo: 'bar' }));
                done();
            });
        });

        it('reopens partition if partition was closed', function() {
            storage = createStorage();
            storage.open();

            const part = storage.getPartition('');
            part.close();
            expect(() => storage.write({ foo: 'bar' })).to.not.throwError();
            expect(storage.index.length).to.be(1);
        });

        it('can partition writes', function(done) {
            storage = createStorage({ partitioner: (doc, number) => 'part-' + ((number-1) % 4) });
            storage.open();

            for (let i = 1; i <= 4; i++) {
                storage.write({ foo: 'bar' }, i === 4 ? () => {
                    expect(fs.existsSync('test/data/storage.part-0')).to.be(true);
                    expect(fs.existsSync('test/data/storage.part-1')).to.be(true);
                    expect(fs.existsSync('test/data/storage.part-2')).to.be(true);
                    expect(fs.existsSync('test/data/storage.part-3')).to.be(true);
                    done();
                } : undefined);
            }
        });

        it('can open secondary indexes lazily', function() {
            storage = createStorage();
            const index = storage.ensureIndex('foo', { type: 'foo' });
            storage.close();

            expect(index.isOpen()).to.be(false);
            for (let i = 1; i <= 10; i++) {
                storage.write({ type: (i % 3) ? 'bar' : 'foo' });
            }
            expect(index.isOpen()).to.be(true);
        });

    });

    describe('length', function() {

        it('returns the amount of documents in the storage', function() {
            storage = createStorage();
            storage.open();

            for (let i = 1; i <= 10; i++) {
                storage.write({ foo: 'bar' });
                expect(storage.length).to.be(i);
            }

            storage.close();
            storage.open();

            expect(storage.length).to.be(10);
        });

    });

    describe('read', function() {

        it('returns false when trying to read out of bounds', function() {
            storage = createStorage();
            storage.open();

            storage.write({ foo: 'bar' });
            expect(storage.read(2)).to.be(false);
        });

        it('can read back written documents', function() {
            storage = createStorage();
            storage.open();

            storage.write({ foo: 'bar' });
            expect(storage.read(1)).to.eql({ foo: 'bar' });

            storage.close();
            storage.open();

            expect(storage.read(1)).to.eql({ foo: 'bar' });
        });

        it('can read back random documents', function() {
            storage = createStorage();
            storage.open();

            for (let i = 1; i <= 10; i++) {
                storage.write({ foo: i });
            }

            expect(storage.read(5)).to.eql({ foo: 5 });

            storage.close();
            storage.open();

            expect(storage.read(5)).to.eql({ foo: 5 });
        });

        it('can read back from partitioned storage', function() {
            storage = createStorage({ partitioner: (doc, number) => 'part-' + ((number-1) % 4) });
            storage.open();

            for (let i = 1; i <= 8; i++) {
                storage.write({ foo: i });
            }

            for (let i = 1; i <= 8; i++) {
                expect(storage.read(i)).to.eql({ foo: i });
            }

            storage.close();
            storage.open();

            for (let i = 1; i <= 8; i++) {
                expect(storage.read(i)).to.eql({ foo: i });
            }

            storage.close();
            storage = createStorage();
            storage.open();

            for (let i = 1; i <= 8; i++) {
                expect(storage.read(i)).to.eql({ foo: i });
            }
        });

        it('can read with using secondary index', function() {
            storage = createStorage();
            storage.open();
            let odd = storage.ensureIndex('odd', (doc) => (doc.foo % 2) === 1);

            for (let i = 1; i <= 10; i++) {
                storage.write({ foo: i });
            }

            expect(storage.read(3, odd)).to.eql({ foo: 5 });

            storage.close();
            storage.open();

            expect(storage.read(3, odd)).to.eql({ foo: 5 });
        });

        it('can open secondary indexes lazily', function() {
            storage = createStorage();
            const index = storage.ensureIndex('foo', { type: 'foo' });
            for (let i = 1; i <= 10; i++) {
                storage.write({ type: (i % 3) ? 'bar' : 'foo' });
            }
            storage.close();

            expect(index.isOpen()).to.be(false);
            storage.read(1, index);
            expect(index.isOpen()).to.be(true);
        });

        it('works with arbitrarily sized documents', function() {
            storage = createStorage({ writeBufferSize: 1024 });
            storage.open();

            for (let i = 1; i <= 10; i++) {
                storage.write({ foo: i, pad: ' '.repeat(storage.partitionConfig.writeBufferSize * i / 12) });
            }

            storage.close();
            storage = createStorage();
            storage.open();

            for (let i = 1; i <= 8; i++) {
                expect(storage.read(i).foo).to.eql(i);
            }
        });

    });

    describe('readRange', function() {

        it('can read full range', function() {
            storage = createStorage();
            storage.open();

            for (let i = 1; i <= 10; i++) {
                storage.write({ foo: i });
            }
            storage.close();
            storage.open();

            let documents = storage.readRange(1);
            let i = 1;
            for (let doc of documents) {
                expect(doc).to.eql({ foo: i++ });
            }
            expect(i).to.be(11);
        });

        it('can read full range in reverse', function() {
            storage = createStorage();
            storage.open();

            for (let i = 1; i <= 20; i++) {
                storage.write({ foo: i });
            }
            storage.close();
            storage.open();

            let i = 20;
            let documents = storage.readRange(i, 1);
            for (let doc of documents) {
                expect(doc).to.eql({ foo: i-- });
            }
            expect(i).to.be(0);
        });

        it('reads all items in reverse when range length is a multiple of batch size', function() {
            // 12 items: sequence 12, 1 (step 11) → old code yielded 2..12 then exited,
            // missing item 1.  Regression test for the >= readUntil boundary fix.
            storage = createStorage();
            storage.open();

            for (let i = 1; i <= 12; i++) {
                storage.write({ foo: i });
            }
            storage.close();
            storage.open();

            let i = 12;
            let documents = storage.readRange(i, 1);
            for (let doc of documents) {
                expect(doc).to.eql({ foo: i-- });
            }
            expect(i).to.be(0);
        });

        it('can read a sub range', function() {
            storage = createStorage();
            storage.open();

            for (let i = 1; i <= 10; i++) {
                storage.write({ foo: i });
            }
            storage.close();
            storage.open();

            let documents = storage.readRange(4, 6);
            let i = 4;
            for (let doc of documents) {
                expect(doc).to.eql({ foo: i++ });
            }
            expect(i).to.be(7);
        });

        it('can read a range from end', function() {
            storage = createStorage();
            storage.open();

            for (let i = 1; i <= 10; i++) {
                storage.write({ foo: i });
            }
            storage.close();
            storage.open();

            let documents = storage.readRange(-4);
            let i = 7;
            for (let doc of documents) {
                expect(doc).to.eql({ foo: i++ });
            }
            expect(i).to.be(11);
        });

        it('can read a range until a position from end', function() {
            storage = createStorage();
            storage.open();

            for (let i = 1; i <= 10; i++) {
                storage.write({ foo: i });
            }
            storage.close();
            storage.open();

            let documents = storage.readRange(1, -4);   // readRange(1, 7)
            let i = 1;
            for (let doc of documents) {
                expect(doc).to.eql({ foo: i++ });
            }
            expect(i).to.be(8);
        });

        it('can read a range from secondary index', function() {
            storage = createStorage();
            storage.open();
            let odd = storage.ensureIndex('odd', (doc) => (doc.foo % 2) === 1);

            for (let i = 1; i <= 10; i++) {
                storage.write({ foo: i });
            }
            storage.close();
            storage.open();

            let documents = storage.readRange(1, 3, odd);
            let i = 1;
            for (let doc of documents) {
                expect(doc).to.eql({ foo: i });
                i += 2;
            }
            expect(i).to.be(7);
        });

        it('throws on invalid range', function() {
            storage = createStorage();
            storage.open();

            for (let i = 1; i <= 10; i++) {
                storage.write({ foo: i });
            }
            storage.close();
            storage.open();

            expect(() => storage.readRange(0).next()).to.throwError();
            expect(() => storage.readRange(11).next()).to.throwError();
            expect(() => storage.readRange(1, 14).next()).to.throwError();
        });

        it('can open secondary indexes lazily', function() {
            storage = createStorage();
            const index = storage.ensureIndex('foo', { type: 'foo' });
            for (let i = 1; i <= 10; i++) {
                storage.write({ type: (i % 3) ? 'bar' : 'foo' });
            }
            storage.close();

            expect(index.isOpen()).to.be(false);
            Array.from(storage.readRange(1, 2, index));
            expect(index.isOpen()).to.be(true);
        });

        it('iterates partitions in sequenceNumber order when index is false', function() {
            storage = createStorage({ partitioner: (doc, number) => 'part-' + ((number - 1) % 3) });
            storage.open();

            for (let i = 1; i <= 9; i++) {
                storage.write({ foo: i });
            }
            storage.close();
            storage.open();

            const documents = Array.from(storage.readRange(1, -1, false));
            expect(documents.length).to.be(9);
            for (let i = 0; i < 9; i++) {
                expect(documents[i]).to.eql({ foo: i + 1 });
            }
        });

        it('iterates partitions in reverse sequenceNumber order when index is false', function() {
            storage = createStorage({ partitioner: (doc, number) => 'part-' + ((number - 1) % 3) });
            storage.open();

            for (let i = 1; i <= 9; i++) {
                storage.write({ foo: i });
            }
            storage.close();
            storage.open();

            const documents = Array.from(storage.readRange(9, 1, false));
            expect(documents.length).to.be(9);
            for (let i = 0; i < 9; i++) {
                expect(documents[i]).to.eql({ foo: 9 - i });
            }
        });

        it('iterates partitions in sequenceNumber order for a sub-range when index is false', function() {
            storage = createStorage({ partitioner: (doc, number) => 'part-' + ((number - 1) % 3) });
            storage.open();

            for (let i = 1; i <= 9; i++) {
                storage.write({ foo: i });
            }
            storage.close();
            storage.open();

            const documents = Array.from(storage.readRange(3, 7, false));
            expect(documents.length).to.be(5);
            for (let i = 0; i < 5; i++) {
                expect(documents[i]).to.eql({ foo: i + 3 });
            }
        });

        it('iteratePartitionsBySequenceNumber yields document, sequenceNumber, partitionName and position', function() {
            storage = createStorage({ partitioner: (doc, number) => 'part-' + ((number - 1) % 3) });
            storage.open();

            for (let i = 1; i <= 6; i++) {
                storage.write({ foo: i });
            }
            storage.close();
            storage.open();

            const entries = Array.from(storage.iteratePartitionsBySequenceNumber(0, 5));
            expect(entries.length).to.be(6);
            for (let i = 0; i < 6; i++) {
                expect(entries[i].document).to.eql({ foo: i + 1 });
                expect(typeof entries[i].sequenceNumber).to.be('number');
                expect(typeof entries[i].partitionName).to.be('string');
                expect(typeof entries[i].position).to.be('number');
                expect(entries[i].sequenceNumber).to.be(i);
            }
            // Verify documents from the same partition have increasing positions
            const byPartition = {};
            for (const entry of entries) {
                if (!byPartition[entry.partitionName]) byPartition[entry.partitionName] = [];
                byPartition[entry.partitionName].push(entry.position);
            }
            for (const positions of Object.values(byPartition)) {
                for (let i = 1; i < positions.length; i++) {
                    expect(positions[i]).to.be.greaterThan(positions[i - 1]);
                }
            }
        });

    });

    describe('ensureIndex', function() {

        it('creates non-existing indexes', function() {
            storage = createStorage();
            storage.open();

            storage.ensureIndex('foo', () => true);
            expect(fs.existsSync('test/data/storage.foo.index')).to.be(true);
        });

        it('returns global index for `_all`', function() {
            storage = createStorage();
            const index = storage.ensureIndex('_all');
            expect(index).to.be(storage.index);
        })

        it('can be called multiple times', function() {
            storage = createStorage();
            storage.open();

            const index1 = storage.ensureIndex('foo', () => true);
            const index2 = storage.ensureIndex('foo', () => true);
            expect(index1).to.be(index2);
        });

        it('throws when calling for non-existing index without matcher', function() {
            storage = createStorage();
            storage.open();

            expect(() => storage.ensureIndex('foo')).to.throwError(/matcher/);
        });

        it('indexes documents by function matcher', function() {
            storage = createStorage();
            storage.open();
            let index = storage.ensureIndex('foo', (doc) => doc.type === 'Foo');
            storage.write({type: 'Bar'});
            storage.write({type: 'Foo'});
            storage.write({type: 'Baz'});
            expect(index.length).to.be(1);
        });

        it('indexes documents by property object matcher', function() {
            storage = createStorage();
            storage.open();
            let index = storage.ensureIndex('foo', { type: 'Foo' });
            storage.write({type: 'Bar'});
            storage.write({type: 'Foo'});
            storage.write({type: 'Baz'});
            expect(index.length).to.be(1);
        });

        it('indexes documents by property object matcher ignoring undefined properties', function(done) {
            storage = createStorage();
            storage.open();
            storage.write({type: 'Bar', other: '1'});
            storage.write({type: 'Foo', other: '2'});
            storage.write({type: 'Baz', other: '3'}, () => {
                let index = storage.ensureIndex('foo', {type: 'Foo', other: undefined});

                expect(index.length).to.be(1);
                done();
            });
        });

        it('reopens existing indexes', function() {
            storage = createStorage();
            storage.open();
            let index = storage.ensureIndex('foo', () => true);
            storage.write({foo: 'bar'});
            expect(index.length).to.be(1);
            storage.close();

            storage = createStorage();
            storage.open();

            index = storage.ensureIndex('foo', () => true);
            expect(index.length).to.be(1);
        });

        it('restores matcher from existing index', function() {
            storage = createStorage();
            storage.open();
            let index = storage.ensureIndex('foo', (doc) => doc.type === 'Foo');
            storage.write({type: 'Foo'});
            expect(index.length).to.be(1);
            storage.close();

            storage = createStorage();
            storage.open();

            index = storage.ensureIndex('foo');
            storage.write({type: 'Foo'});
            expect(index.length).to.be(2);
        });

        it('throws when hmac does not validate matcher from existing index', function() {
            storage = createStorage({ hmacSecret: 'foo' });
            storage.open();
            storage.ensureIndex('foo-hmac', (doc) => doc.type === 'Foo');
            storage.close();

            storage = createStorage({ hmacSecret: 'bar' });
            storage.open();

            expect(() => storage.ensureIndex('foo-hmac', (doc) => doc.type === 'Foo')).to.throwError();
        });

        it('throws when reopening with different matcher', function() {
            storage = createStorage();
            storage.open();
            storage.ensureIndex('foo-matcher', () => true);
            storage.close();

            storage = createStorage();
            storage.open();

            expect(() => storage.ensureIndex('foo-matcher', (doc) => doc.type === 'Foo')).to.throwError();
        });

        it('does not create an index when filling it fails', function() {
            storage = createStorage();
            storage.open();
            storage.write({type: 'Foo'});

            const Index = require('../src/Index');
            const originalAdd = Index.prototype.add;
            Index.prototype.add = () => { throw new Error('Failure'); };
            try {
                storage.ensureIndex('foo-new', (doc) => doc.type === 'Foo');
            } catch (e) {}
            expect(fs.existsSync('test/data/storage.foo-new.index')).to.be(false);
            Index.prototype.add = originalAdd;
        });

    });

    describe('openIndex', function() {

        it('throws on non-existing indexes', function () {
            storage = createStorage();
            storage.open();

            expect(() => storage.openIndex('foo')).to.throwError(/does not exist/);
        });

        it('throws when hmac does not validate matcher from existing index', function () {
            storage = createStorage({ hmacSecret: 'foo'});
            storage.open();
            storage.ensureIndex('foo', (doc) => doc.type === 'Foo');
            storage.close();

            storage = createStorage({ hmacSecret: 'bar'});
            storage.open();
            expect(() => storage.openIndex('foo')).to.throwError(/HMAC/);
        });

        it('opens existing indexes', function () {
            storage = createStorage();
            storage.open();
            storage.ensureIndex('foo', (doc) => doc.type === 'Foo');
            storage.write({type: 'Foo'});
            storage.close();

            storage = createStorage();
            storage.open();
            const index = storage.openIndex('foo');
            expect(index.length).to.be(1);
        });

    });

    describe('truncate', function() {

        it('does nothing if truncating after the current position', function() {
            storage = createStorage();
            storage.open();

            for (let i = 1; i <= 10; i++) {
                storage.write({ foo: i });
            }
            storage.close();
            storage.open();

            storage.truncate(12);

            expect(storage.length).to.be(10);
        });

        it('correctly truncates to empty', function() {
            storage = createStorage();
            storage.open();

            for (let i = 1; i <= 10; i++) {
                storage.write({ foo: i });
            }
            storage.close();
            storage.open();

            storage.truncate(0);

            expect(storage.length).to.be(0);
        });

        it('truncates after the given document number', function() {
            storage = createStorage();
            storage.open();

            for (let i = 1; i <= 10; i++) {
                storage.write({ foo: i });
            }
            storage.close();
            storage.open();

            storage.truncate(6);

            expect(storage.length).to.be(6);

            let documents = storage.readRange(1);
            let i = 1;
            for (let doc of documents) {
                expect(doc).to.eql({ foo: i++ });
            }
            expect(i).to.be(7);
        });

        it('truncates after the given document number on each partition', function() {
            storage = createStorage({
                partitioner: (doc, number) => 'part-' + (parseInt((number - 1) / 4) % 4)
            });
            storage.open();

            for (let i = 1; i <= 10; i++) {
                storage.write({foo: i});
            }

            storage.close();
            storage.open();

            storage.truncate(6);

            expect(storage.length).to.be(6);

            let documents = storage.readRange(1);
            let i = 1;
            for (let doc of documents) {
                expect(doc).to.eql({ foo: i++ });
            }
            expect(i).to.be(7);
        });

        it('truncates secondary indexes correctly', function() {
            storage = createStorage();
            storage.open();
            let index = storage.ensureIndex('foobar', (doc) => doc.foo % 2 === 0);

            for (let i = 1; i <= 10; i++) {
                storage.write({foo: i});
            }

            storage.close();
            storage.open();

            storage.truncate(6);

            expect(storage.length).to.be(6);
            expect(index.length).to.be(3);

            let documents = storage.readRange(1, -1, index);
            let i = 2;
            for (let doc of documents) {
                expect(doc).to.eql({ foo: i });
                i += 2;
            }
            expect(i).to.be(8);
        });

        it('keeps truncated secondary indexes closed', function() {
            storage = createStorage();
            storage.open();
            let index = storage.ensureIndex('foobar', (doc) => doc.foo % 2 === 0);

            for (let i = 1; i <= 10; i++) {
                storage.write({foo: i});
            }

            storage.close();

            storage.truncate(6);

            expect(index.isOpen()).to.be(false);
            index.open();
            expect(index.length).to.be(3);
        });

        it('repairs stale secondary index when opened after a primary index truncation', function() {
            // Simulate the case where checkTornWrites() truncated the primary index before
            // secondary indexes were loaded (e.g. after LOCK_RECLAIM on an unclean shutdown).
            storage = createStorage();
            storage.open();
            // Write 10 documents and ensure a secondary index (matches even-numbered foo)
            const totalDocuments = 10;
            const truncateAt = 6; // simulated truncation point
            const expectedSecondaryCount = Math.floor(truncateAt / 2); // even numbers in 1..truncateAt
            storage.ensureIndex('foobar', (doc) => doc.foo % 2 === 0);
            for (let i = 1; i <= totalDocuments; i++) {
                storage.write({foo: i});
            }
            storage.flush();
            storage.close();

            // Re-open and truncate primary index directly (simulating checkTornWrites running
            // before secondary indexes are loaded)
            storage = createStorage();
            storage.open();
            storage.index.truncate(truncateAt); // truncate primary index without going through full truncate()
            // Close without touching secondary index on disk
            storage.index.flush();
            storage.close();

            // Re-open: openIndex() should detect the stale secondary index and repair it
            storage = createStorage();
            storage.open();
            const repairedIndex = storage.openIndex('foobar');
            expect(repairedIndex.length).to.be(expectedSecondaryCount); // only entries 2,4,6 are still valid
        });
    });

    describe('checkTornWrites', function() {

        it('auto-repairs primary index when it is lagging behind partition data', function() {
            storage = createStorage();
            storage.open();

            for (let i = 1; i <= 5; i++) {
                storage.write({ foo: i });
            }
            storage.flush();

            // Simulate: partition was flushed but index was only partially flushed (3 of 5 entries)
            storage.index.truncate(3);
            storage.index.flush();

            storage.checkTornWrites();

            // After auto-repair, all 5 documents should be accessible via primary index.
            expect(storage.index.length).to.be(5);
            for (let i = 1; i <= 5; i++) {
                expect(storage.read(i)).to.eql({ foo: i });
            }
        });

        it('does not reindex when the index is up to date', function() {
            storage = createStorage();
            storage.open();

            for (let i = 1; i <= 5; i++) {
                storage.write({ foo: i });
            }
            storage.flush();

            const lengthBefore = storage.index.length;

            storage.checkTornWrites();

            expect(storage.index.length).to.be(lengthBefore);
        });

        it('does not reindex when torn write is present and index is up to date', function() {
            storage = createStorage();
            storage.open();

            for (let i = 1; i <= 5; i++) {
                storage.write({ foo: i });
            }
            storage.flush();
            storage.close();

            // Simulate torn write: truncate partition file to remove last document's footer (8 bytes)
            const partitionName = path.join(dataDirectory, 'storage');
            const fd = fs.openSync(partitionName, 'r+');
            const stat = fs.fstatSync(fd);
            fs.ftruncateSync(fd, stat.size - 8);
            fs.closeSync(fd);

            storage = createStorage();
            storage.open();

            storage.checkTornWrites();

            // Doc5 was torn and removed; index should now have exactly 4 entries.
            expect(storage.index.length).to.be(4);
        });

        it('auto-repairs primary index when both torn write and index lag are present', function() {
            storage = createStorage();
            storage.open();

            for (let i = 1; i <= 5; i++) {
                storage.write({ foo: i });
            }
            storage.flush();
            storage.close();

            // Simulate torn write on last doc AND a lagging index (only 3 of 5 entries flushed)
            const partitionName = path.join(dataDirectory, 'storage');
            const fd = fs.openSync(partitionName, 'r+');
            const stat = fs.fstatSync(fd);
            fs.ftruncateSync(fd, stat.size - 8); // remove DOCUMENT_FOOTER_SIZE (8) bytes
            fs.closeSync(fd);

            // Re-open and truncate index to 3 to simulate index lag
            storage = createStorage();
            storage.open();
            storage.index.truncate(3);
            storage.index.flush();
            storage.close();

            storage = createStorage();
            storage.open();

            storage.checkTornWrites();

            // After torn write removal: doc5 is gone. Auto-repair rebuilds the missing entry
            // for doc4, leaving the index with 4 entries for docs 1-4.
            expect(storage.index.length).to.be(4);
            for (let i = 1; i <= 4; i++) {
                expect(storage.read(i)).to.eql({ foo: i });
            }
        });

    });

    describe('reindex', function() {

        it('rebuilds primary index from scratch when called with 0', function() {
            storage = createStorage();
            storage.open();

            for (let i = 1; i <= 5; i++) {
                storage.write({ foo: i });
            }
            storage.flush();

            // Simulate index corruption: truncate to empty
            storage.index.truncate(0);
            storage.index.flush();

            storage.reindex(0);

            expect(storage.index.length).to.be(5);
            for (let i = 1; i <= 5; i++) {
                expect(storage.read(i)).to.eql({ foo: i });
            }
        });

        it('rebuilds primary index from a given position', function() {
            storage = createStorage();
            storage.open();

            for (let i = 1; i <= 5; i++) {
                storage.write({ foo: i });
            }
            storage.flush();

            // Simulate partial index loss: truncate to 3 entries
            storage.index.truncate(3);
            storage.index.flush();

            storage.reindex(3);

            expect(storage.index.length).to.be(5);
            for (let i = 1; i <= 5; i++) {
                expect(storage.read(i)).to.eql({ foo: i });
            }
        });

        it('rebuilds primary index across multiple partitions', function() {
            storage = createStorage({ partitioner: (doc, number) => 'part-' + ((number - 1) % 3) });
            storage.open();

            for (let i = 1; i <= 9; i++) {
                storage.write({ foo: i });
            }
            storage.flush();

            storage.index.truncate(4);
            storage.index.flush();

            storage.reindex(4);

            expect(storage.index.length).to.be(9);
            for (let i = 1; i <= 9; i++) {
                expect(storage.read(i)).to.eql({ foo: i });
            }
        });

        it('rebuilds loaded secondary indexes from given position', function() {
            storage = createStorage();
            storage.open();

            storage.ensureIndex('evens', doc => doc.foo % 2 === 0);
            for (let i = 1; i <= 6; i++) {
                storage.write({ foo: i });
            }
            storage.flush();

            // Truncate both primary and secondary indexes to 3 entries
            storage.index.truncate(3);
            storage.index.flush();
            const secIndex = storage.secondaryIndexes['evens'].index;
            secIndex.truncate(1); // keep only entry for foo=2
            secIndex.flush();

            storage.reindex(3);

            // Primary should have all 6 entries
            expect(storage.index.length).to.be(6);
            // Secondary should have entries for foo=2, foo=4, foo=6 (3 total)
            expect(secIndex.length).to.be(3);
            // Verify documents accessible via secondary index
            expect(storage.read(1, secIndex)).to.eql({ foo: 2 });
            expect(storage.read(2, secIndex)).to.eql({ foo: 4 });
            expect(storage.read(3, secIndex)).to.eql({ foo: 6 });
        });

        it('rebuilds all secondary indexes from scratch when called with 0', function() {
            storage = createStorage();
            storage.open();

            storage.ensureIndex('odds', doc => doc.foo % 2 !== 0);
            for (let i = 1; i <= 6; i++) {
                storage.write({ foo: i });
            }
            storage.flush();

            const secIndex = storage.secondaryIndexes['odds'].index;
            secIndex.truncate(0);
            secIndex.flush();
            storage.index.truncate(0);
            storage.index.flush();

            storage.reindex(0);

            expect(storage.index.length).to.be(6);
            expect(secIndex.length).to.be(3); // foo=1, foo=3, foo=5
        });

    });

    describe('open (ready event)', function() {

        it('emits ready event after opening', function(done) {
            storage = createStorage();
            storage.on('ready', done);
            storage.open();
        });

        it('emits ready event after auto-repairing a lagging primary index', function(done) {
            // Simulate a crashed writer: write data, partially flush index, but leave the lock
            // file in place (do NOT call close(), which would remove the lock).
            const crashedStorage = new Storage({ dataDirectory });
            crashedStorage.open();
            for (let i = 1; i <= 5; i++) {
                crashedStorage.write({ foo: i });
            }
            crashedStorage.flush();
            crashedStorage.index.truncate(3);
            crashedStorage.index.flush();
            // Close just the file handles to avoid fd conflicts, but leave the lock file in place
            // so LOCK_RECLAIM knows it needs to run checkTornWrites().
            crashedStorage.index.close();
            crashedStorage.forEachPartition(partition => partition.close());
            // (crashedStorage.locked remains true; lock file on disk is NOT removed)

            // Reopen with LOCK_RECLAIM — detects the orphaned lock, runs checkTornWrites()
            // (which calls reindex()), and then open() emits 'ready'.
            const repairedStorage = new Storage({ dataDirectory, lock: Storage.LOCK_RECLAIM });
            refs.push(repairedStorage);
            let readyFired = false;
            repairedStorage.on('ready', () => {
                readyFired = true;
                // Index should be fully repaired by the time 'ready' fires
                expect(repairedStorage.index.length).to.be(5);
                done();
            });
            repairedStorage.open();
            // Note: 'ready' is emitted synchronously inside open(), so readyFired is already true
            expect(readyFired).to.be(true);
            // Prevent afterEach from trying to remove the now-gone lock file
            crashedStorage.locked = false;
        });

    });

    describe('matches', function() {

        const matches = Storage.matches;

        it('returns true if no matcher specified', function() {
            expect(matches({ foo: 'bar' })).to.be(true);
        });

        it('returns false if no document specified', function() {
            expect(matches()).to.be(false);
        });

        it('works with object matchers', function() {
            expect(matches({ foo: 'foo', bar: { baz: 'baz', quux: 'quux' } }, { foo: 'foo', bar: { baz: 'baz' } })).to.be(true);
            expect(matches({ foo: 'foo', bar: { baz: 'baz2', quux: 'quux' } }, { foo: 'foo', bar: { baz: 'baz' } })).to.be(false);
        });

        it('works with function matchers', function() {
            expect(matches({ foo: 'foo', bar: { baz: 'baz', quux: 'quux' } }, (doc) => doc.foo === 'foo')).to.be(true);
            expect(matches({ foo: 'foo2', bar: { baz: 'baz', quux: 'quux' } }, (doc) => doc.foo === 'foo')).to.be(false);
        });

    });

    describe('forEachDocument', function() {

        it('does nothing when called on empty storage', function(done) {
            storage = createStorage();
            storage.open();

            storage.forEachDocument((doc) => expect(this).to.be(false));
            setTimeout(done, 1);
        });

    });

    it('works with compression applied in serializer', function() {
        const dictionary = Buffer.from('"metadata":{"committedAt":,"commitId":,"commitVersion":,"streamVersion":{"stream":"Event-0f0da93d-260a-4ffd-94a0-126bd67ee8ff","payload":{"type":,"metadata":{"occuredAt":');
        storage = createStorage({ serializer: {
            serialize: (doc) => {
                return zlib.deflateRawSync(Buffer.from(JSON.stringify(doc)), { dictionary }).toString('binary');
                //return lz4.encode(Buffer.from(JSON.stringify(doc))).toString('binary');
            },
            deserialize: (string) => {
                return JSON.parse(zlib.inflateRawSync(Buffer.from(string, 'binary'), { dictionary }).toString());
                //return JSON.parse(lz4.decode(Buffer.from(string, 'binary')));
            }
        }});
        storage.open();

        const doc = {"stream":"Event-0f0da93d-260a-4ffd-94a0-126bd67ee8ff","payload":{"type":"DomainEvent","payload":{"type":"EventCreated","payload":{"eventIdentifier":"0f0da93d-260a-4ffd-94a0-126bd67ee8ff","name":"testi"}},"metadata":{"occuredAt":1482764928713,"aggregateId":"0f0da93d-260a-4ffd-94a0-126bd67ee8ff","version":0,"type":"EventCreated","correlationId":"9242b72b-03a0-4078-ae62-f37af7cb1b39"}},"metadata":{"committedAt":1482764928715,"commitId":1,"commitVersion":0,"streamVersion":0}};
        storage.write(doc);
        storage.write(doc);
        expect(storage.read(1)).to.be.eql(doc);
        expect(storage.read(2)).to.be.eql(doc);
    });

    describe('concurrency', function() {

        it('allows multiple writers to different partitions', function () {
            // t.b.d. - only possible if there is no storage global index
        });

        it('allows only a single writer', function(){
            let storage2 = createStorage();
            storage2.open();
            expect(() => {
                storage = createStorage();
                storage.open();
            }).to.throwError(e => e instanceof Storage.StorageLockedError);
            storage2.close();
        });

        it('releases write lock after closing', function(){
            const storage2 = createStorage();
            storage2.open();
            storage2.close();
            expect(() => {
                storage = createStorage();
                storage.open();
            }).to.not.throwError();
        });

        it('allows multiple readers for one storage', function () {
            storage = createStorage();
            storage.open();
            for (let i = 1; i <= 10; i++) {
                storage.write({foo: i});
            }
            storage.flush();

            let reader1 = createReader();
            reader1.open();
            expect(reader1.length).to.be(storage.length);

            let reader2 = createReader();
            reader2.open();
            expect(reader2.length).to.be(storage.length);

            reader1.close();
            reader2.close();
        });

    });

    describe('ReadOnly', function(){

        it('triggers event when writer appends', function(done){
            storage = createStorage({ syncOnFlush: true });
            storage.open();
            for (let i = 1; i <= 10; i++) {
                storage.write({foo: i});
            }
            storage.flush();

            let reader = createReader();
            reader.open();
            reader.on('index-add', () => expect(this).to.be(false));
            reader.on('wrote', (doc, entry, position) => {
                expect(entry.number).to.be(11);
                expect(doc).to.eql({foo: 11});
                reader.close();
                done();
            });
            expect(reader.length).to.be(storage.length);

            storage.write({ foo: 11 });
            storage.flush();
        });

        it('updates secondary indexes when writer appends', function(done){
            storage = createStorage({ syncOnFlush: true });
            storage.open();
            storage.ensureIndex('foo', doc => doc.type === 'foo');
            for (let i = 1; i <= 10; i++) {
                storage.write({foo: i, type: 'foo'});
            }
            storage.flush();

            let reader = createReader();
            reader.open();
            reader.openIndex('foo');
            reader.on('index-add', (name, number, doc) => {
                expect(name).to.be('foo');
                expect(number).to.be(11);
                expect(doc).to.eql({foo: 11, type: 'foo'});
                reader.close();
                done();
            });
            expect(reader.length).to.be(storage.length);

            storage.write({ foo: 11, type: 'foo' });
            storage.flush();
        });

        it('triggers event when writer truncates', function(done){
            storage = createStorage({ syncOnFlush: true });
            storage.open();
            for (let i = 1; i <= 10; i++) {
                storage.write({foo: i});
            }
            storage.flush();

            let reader = createReader();
            reader.open();
            reader.on('truncate', (prevLength, newLength) => {
                expect(prevLength).to.be(10);
                expect(newLength).to.be(4);
                reader.close();
                done();
            });
            expect(reader.length).to.be(storage.length);

            storage.truncate(4);
        });

        it('does not trigger truncate for secondary indexes', function(done){
            storage = createStorage({ syncOnFlush: true });
            storage.open();
            storage.ensureIndex('foo', doc => doc.type === 'foo');
            for (let i = 1; i <= 10; i++) {
                storage.write({foo: i, type: i > 5 ? 'foo' : 'bar'});
            }
            storage.flush();

            let reader = createReader();
            reader.open();
            reader.on('truncate', (prevLength, newLength) => {
                expect(prevLength).to.be(10);
                expect(newLength).to.be(4);
                setTimeout(() => {
                    reader.close();
                    done();
                }, 5);
            });
            expect(reader.length).to.be(storage.length);

            storage.truncate(4);
        });

        it('recognizes new indexes created by writer', function(done){
            storage = createStorage({ syncOnFlush: true, partitioner:  (document, number) => document.type });
            storage.open();

            let reader = createReader();
            reader.open();
            reader.on('index-created', (name) => {
                expect(name).to.be('one');
                expect(reader.secondaryIndexes[name]).to.be(undefined);
                reader.close();
                done();
            });

            storage.ensureIndex('one', doc => doc.type === 'one');
            storage.flush();
        });

        it('recognizes new indexes created in different directory by writer', function(done){
            storage = createStorage({ indexDirectory: dataDirectory + '/indexes', syncOnFlush: true, partitioner:  (document, number) => document.type });
            storage.open();

            let reader = createReader( { indexDirectory: dataDirectory + '/indexes' });
            reader.open();
            reader.on('index-created', (name) => {
                expect(name).to.be('one');
                expect(reader.secondaryIndexes[name]).to.be(undefined);
                reader.close();
                done();
            });

            storage.ensureIndex('one', doc => doc.type === 'one');
            storage.flush();
        });

        it('ignores new indexes created by other storage', function(done){
            storage = createStorage();
            storage.open();
            storage.close();

            storage = new Storage('other-storage', { dataDirectory, syncOnFlush: true, partitioner:  (document, number) => document.type });
            storage.open();

            let reader = createReader();
            reader.open();
            reader.on('index-created', (name) => {
                expect(this).to.be(false);
                reader.close();
            });

            storage.write({ foo: 1, type: 'one' });
            storage.ensureIndex('one', doc => doc.type === 'one');
            storage.flush();
            setTimeout(() => {
                reader.close();
                done();
            }, 5);
        });

        it('recognizes new partitions created by writer', function(done){
            storage = createStorage({ syncOnFlush: true, partitioner:  (document, number) => document.type });
            storage.open();

            let reader = createReader();
            reader.open();
            reader.on('partition-created', (id) => {
                expect(reader.getPartition(id).name.substr(-3)).to.be('one');
                reader.close();
                done();
            });

            storage.write({ foo: 1, type: 'one' });
            storage.flush();
        });

        it('ignores new partitions created by other storage', function(done){
            storage = createStorage();
            storage.open();
            storage.close();

            storage = new Storage('other-storage', { dataDirectory, syncOnFlush: true, partitioner:  (document, number) => document.type });
            storage.open();

            let reader = createReader();
            reader.open();
            reader.on('partition-created', (name) => {
                expect(this).to.be(false);
                reader.close();
            });

            storage.write({ foo: 1, type: 'one' });
            storage.flush();
            setTimeout(() => {
                reader.close();
                done();
            }, 5);
        });

        it('can be opened and closed multiple times', function(){
            storage = createStorage();
            storage.open();

            let reader = createReader();
            reader.open();
            expect(reader.open()).to.be(true);
            reader.close();
            reader.close();
        });

        it('partitions are opened only once', function(done){
            storage = createStorage({ syncOnFlush: true, partitioner:  (document, number) => document.type });
            storage.open();

            let reader = createReader();
            reader.open();
            reader.on('partition-created', (id) => {
                const partitionInstance = reader.getPartition(id);
                expect(reader.getPartition(id)).to.be(partitionInstance);
                reader.close();
                done();
            });

            storage.getPartition('');
        });
    });

    describe('preCommit', function() {

        it('calls the hook before writing with the document and partition metadata', function() {
            storage = createStorage({ metadata: { allowedRoles: ['admin'] } });
            storage.open();

            let hookDocument, hookMetadata;
            storage.preCommit((document, partitionMetadata) => {
                hookDocument = document;
                hookMetadata = partitionMetadata;
            });

            storage.write({ foo: 'bar' });

            expect(hookDocument).to.eql({ foo: 'bar' });
            expect(hookMetadata.allowedRoles).to.eql(['admin']);
        });

        it('aborts the write when the hook throws', function() {
            storage = createStorage();
            storage.open();

            storage.preCommit(() => {
                throw new Error('not allowed');
            });

            expect(() => storage.write({ foo: 'bar' })).to.throwError(/not allowed/);
            expect(storage.length).to.be(0);
        });

        it('allows writes when the hook does not throw', function() {
            storage = createStorage({ metadata: { allowedRoles: ['admin'] } });
            storage.open();

            const globalContext = { authorizedRoles: ['admin', 'user'] };
            storage.preCommit((document, partitionMetadata) => {
                if (!partitionMetadata.allowedRoles.some(role => globalContext.authorizedRoles.includes(role))) {
                    throw new Error('Not allowed');
                }
            });

            expect(storage.write({ foo: 'bar' })).to.be(1);
        });

        it('uses per-partition metadata when config.metadata is a function', function() {
            storage = createStorage({
                partitioner: (doc) => doc.type,
                metadata: (partitionName) => ({ allowedRoles: partitionName === 'admin' ? ['admin'] : ['user'] })
            });
            storage.open();

            const calls = [];
            storage.preCommit((document, partitionMetadata) => {
                calls.push({ document, metadata: partitionMetadata });
            });

            storage.write({ foo: 1, type: 'admin' });
            storage.write({ foo: 2, type: 'user' });

            expect(calls.length).to.be(2);
            expect(calls[0].metadata.allowedRoles).to.eql(['admin']);
            expect(calls[1].metadata.allowedRoles).to.eql(['user']);
        });

    });

    describe('preRead', function() {

        it('calls the hook before reading with the position and partition metadata', function() {
            storage = createStorage({ metadata: { allowedRoles: ['admin'] } });
            storage.open();
            storage.write({ foo: 'bar' });

            let hookPosition, hookMetadata;
            storage.preRead((position, partitionMetadata) => {
                hookPosition = position;
                hookMetadata = partitionMetadata;
            });

            const result = storage.read(1);

            expect(result).to.eql({ foo: 'bar' });
            expect(typeof hookPosition).to.be('number');
            expect(hookMetadata.allowedRoles).to.eql(['admin']);
        });

        it('aborts the read when the hook throws', function() {
            storage = createStorage();
            storage.open();
            storage.write({ foo: 'bar' });

            storage.preRead(() => {
                throw new Error('read not allowed');
            });

            expect(() => storage.read(1)).to.throwError(/read not allowed/);
        });

        it('calls the hook for each document in a range read', function() {
            storage = createStorage({ metadata: { allowedRoles: ['user'] } });
            storage.open();

            for (let i = 1; i <= 3; i++) {
                storage.write({ foo: i });
            }

            const hookCalls = [];
            storage.preRead((position, partitionMetadata) => {
                hookCalls.push({ position, metadata: partitionMetadata });
            });

            const docs = Array.from(storage.readRange(1, 3));
            expect(docs.length).to.be(3);
            expect(hookCalls.length).to.be(3);
            expect(hookCalls[0].metadata.allowedRoles).to.eql(['user']);
        });

    });
});
