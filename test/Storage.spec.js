const expect = require('expect.js');
const fs = require('fs-extra');
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

        it('invokes indexers and correctly indexes documents', function() {
            const sampleIndexer = (document) => {
                const name = 'type-' + document.type;
                const matcher = { type: document.type };
                return {name, matcher};
            };
            storage = createStorage();
            storage.addIndexer(sampleIndexer);
            storage.addIndexer((document) => null);
            storage.addIndexer((document) => ({ name: null, matcher: null }));

            expect(() => storage.openIndex('type-bar')).to.throwError();
            expect(() => storage.openIndex('type-foo')).to.throwError();
            for (let i = 1; i <= 10; i++) {
                storage.write({ type: (i % 3) ? 'bar' : 'foo', id: i });
            }
            expect(Object.keys(storage.secondaryIndexes)).to.eql(['type-bar', 'type-foo']);
            const barIndex = storage.openIndex('type-bar');
            const fooIndex = storage.openIndex('type-foo');

            expect(barIndex.length).to.be(7);
            expect(fooIndex.length).to.be(3);
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

    });

    describe('ensureIndex', function() {

        it('creates non-existing indexes', function() {
            storage = createStorage();
            storage.open();

            storage.ensureIndex('foo', () => true);
            expect(fs.existsSync('test/data/storage.foo.index')).to.be(true);
        });

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

    });
});
