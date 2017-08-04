const expect = require('expect.js');
const fs = require('fs-extra');
const Storage = require('../src/Storage');

const dataDir = __dirname + '/data';

describe('Storage', function() {

    let storage;

    beforeEach(function () {
        fs.emptyDirSync(dataDir);
    });

    afterEach(function () {
        if (storage) storage.close();
        storage = undefined;
    });

    describe('write', function(done) {

        it('writes objects', function(done) {
            storage = new Storage({ dataDirectory: dataDir });
            storage.open();

            storage.write({ foo: 'bar' }, done);
        });

        it('writes documents sequentially', function() {
            storage = new Storage({ dataDirectory: dataDir });
            storage.open();

            for (let i = 1; i <= 10; i++) {
                expect(storage.write({ foo: 'bar' })).to.be(i);
            }
            expect(storage.index.length).to.be(10);
        });

        it('can write durable', function(done) {
            storage = new Storage({ dataDirectory: dataDir, maxWriteBufferDocuments: 1, syncOnFlush: true });
            storage.open();

            storage.write({ foo: 'bar' }, () => {
                let fileContent = fs.readFileSync('test/data/storage', 'utf8');
                expect(fileContent).to.contain(JSON.stringify({ foo: 'bar' }));
                done();
            });
        });

        it('can partition writes', function(done) {
            storage = new Storage({ dataDirectory: dataDir, partitioner: (doc, number) => 'part-' + ((number-1) % 4) });
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

    });

    describe('length', function() {

        it('returns the amount of documents in the storage', function() {
            storage = new Storage({ dataDirectory: dataDir });
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

        it('can read back written documents', function() {
            storage = new Storage({ dataDirectory: dataDir });
            storage.open();

            storage.write({ foo: 'bar' });
            expect(storage.read(1)).to.eql({ foo: 'bar' });

            storage.close();
            storage.open();

            expect(storage.read(1)).to.eql({ foo: 'bar' });
        });

        it('can read back random documents', function() {
            storage = new Storage({ dataDirectory: dataDir });
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
            storage = new Storage({ dataDirectory: dataDir, partitioner: (doc, number) => 'part-' + ((number-1) % 4) });
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
            storage = new Storage({ dataDirectory: dataDir });
            storage.open();

            for (let i = 1; i <= 8; i++) {
                expect(storage.read(i)).to.eql({ foo: i });
            }
        });

        it('can read with using secondary index', function() {
            storage = new Storage({ dataDirectory: dataDir });
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

    });

    describe('readRange', function() {

        it('can read full range', function() {
            storage = new Storage({ dataDirectory: dataDir });
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

        it('can read a sub range', function() {
            storage = new Storage({ dataDirectory: dataDir });
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
            storage = new Storage({ dataDirectory: dataDir });
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
            storage = new Storage({ dataDirectory: dataDir });
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
            storage = new Storage({ dataDirectory: dataDir });
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
            storage = new Storage({ dataDirectory: dataDir });
            storage.open();

            for (let i = 1; i <= 10; i++) {
                storage.write({ foo: i });
            }
            storage.close();
            storage.open();

            expect(() => storage.readRange(0).next()).to.throwError();
            expect(() => storage.readRange(11).next()).to.throwError();
            expect(() => storage.readRange(1, 14).next()).to.throwError();
            expect(() => storage.readRange(8, 4).next()).to.throwError();
        });

    });

    describe('ensureIndex', function() {

        it('creates non-existing indexes', function() {
            storage = new Storage({ dataDirectory: dataDir });
            storage.open();

            storage.ensureIndex('foo', () => true);
            expect(fs.existsSync('test/data/storage.foo.index')).to.be(true);
        });

        it('indexes documents by function matcher', function() {
            storage = new Storage({ dataDirectory: dataDir });
            storage.open();
            let index = storage.ensureIndex('foo', (doc) => doc.type === 'Foo');
            storage.write({type: 'Bar'});
            storage.write({type: 'Foo'});
            storage.write({type: 'Baz'});
            expect(index.length).to.be(1);
        });

        it('indexes documents by property object matcher', function() {
            storage = new Storage({ dataDirectory: dataDir });
            storage.open();
            let index = storage.ensureIndex('foo', { type: 'Foo' });
            storage.write({type: 'Bar'});
            storage.write({type: 'Foo'});
            storage.write({type: 'Baz'});
            expect(index.length).to.be(1);
        });

        it('reopens existing indexes', function() {
            storage = new Storage({ dataDirectory: dataDir });
            storage.open();
            let index = storage.ensureIndex('foo', () => true);
            storage.write({foo: 'bar'});
            expect(index.length).to.be(1);
            storage.close();

            storage = new Storage({ dataDirectory: dataDir });
            storage.open();

            index = storage.ensureIndex('foo', () => true);
            expect(index.length).to.be(1);
        });

        it('restores matcher from existing index', function() {
            storage = new Storage({ dataDirectory: dataDir });
            storage.open();
            let index = storage.ensureIndex('foo', (doc) => doc.type === 'Foo');
            storage.write({type: 'Foo'});
            expect(index.length).to.be(1);
            storage.close();

            storage = new Storage({ dataDirectory: dataDir });
            storage.open();

            index = storage.ensureIndex('foo');
            storage.write({type: 'Foo'});
            expect(index.length).to.be(2);
        });

        it('throws when hmac does not validate matcher from existing index', function() {
            storage = new Storage({ dataDirectory: dataDir, privateKey: 'foo' });
            storage.open();
            let index = storage.ensureIndex('foo', (doc) => doc.type === 'Foo');
            storage.write({type: 'Foo'});
            expect(index.length).to.be(1);
            storage.close();

            storage = new Storage({ dataDirectory: dataDir, privateKey: 'bar' });
            storage.open();

            expect(() => storage.ensureIndex('foo', (doc) => doc.type === 'Foo')).to.throwError();
        });

        it('throws when reopening with different matcher', function() {
            storage = new Storage({ dataDirectory: dataDir });
            storage.open();
            let index = storage.ensureIndex('foo', () => true);
            storage.close();

            storage = new Storage({ dataDirectory: dataDir });
            storage.open();

            expect(() => storage.ensureIndex('foo', (doc) => doc.type === 'Foo')).to.throwError();
        });

    });

    describe('truncate', function() {

        it('correctly truncates to empty', function() {
            storage = new Storage({ dataDirectory: dataDir });
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
            storage = new Storage({ dataDirectory: dataDir });
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
            storage = new Storage({
                dataDirectory: dataDir,
                partitioner: (doc, number) => 'part-' + ((number - 1) % 4)
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
            storage = new Storage({ dataDirectory: dataDir });
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

    });

});
