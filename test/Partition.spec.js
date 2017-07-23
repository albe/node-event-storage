const expect = require('expect.js');
const fs = require('fs-extra');
const Partition = require('../src/Partition');

describe('Partition', function() {

    let partition;

    beforeEach(function () {
        fs.emptyDirSync('test/data');
        partition = new Partition('.part', { dataDirectory: 'test/data' });
    });

    afterEach(function () {
        if (partition) partition.close();
        partition = undefined;
    });

    it('is not opened automatically on construct', function() {
        expect(partition.isOpen()).to.be(false);
    });

    it('throws on opening non-partition file', function() {
        let fd = fs.openSync('test/data/.part', 'w');
        fs.writeSync(fd, 'foobar');
        fs.closeSync(fd);

        expect(() => partition.open()).to.throwError();
    });

    describe('write', function() {

        it('returns false when partition is not open', function() {
            expect(partition.write('foo')).to.be(false);
        });

        it('calls callback eventually', function(done) {
            partition.open();
            partition.write('foo', done);
        });

        it('writes data sequentially', function() {
            partition.open();
            for (let i = 1; i <= 50; i++) {
                partition.write('foo-' + i.toString());
            }
            partition.close();
            partition.open();
            let i = 1;
            for (let data of partition.readAll()) {
                expect(data).to.be('foo-' + i.toString());
                i++;
            }
        });

        it('writes utf-8 data correctly', function() {
            partition.open();
            let doc1 = partition.write('foo-üöälß');
            let doc2 = partition.write('bar-日本語');
            partition.close();
            partition.open();
            expect(partition.readFrom(doc1)).to.be('foo-üöälß');
            expect(partition.readFrom(doc2)).to.be('bar-日本語');
        });

        it('returns file position of written document', function() {
            partition.open();
            let positions = [];
            for (let i = 1; i <= 10; i++) {
                positions.push(partition.write('foo-' + i.toString()));
            }
            partition.close();
            partition.open();
            for (let i = 1; i <= positions.length; i++) {
                expect(partition.readFrom(positions[i - 1])).to.be('foo-' + i.toString());
            }
        });

        it('can write large documents', function(done) {
            partition.open();
            let blob = 'foobar'.repeat(100000);
            partition.write(blob, () => {
                let stat = fs.statSync('test/data/.part');
                expect(stat.size).to.be.greaterThan(blob.length);
                done();
            });
        });

    });

    describe('readFrom', function() {

        it('returns false when partition is not open', function() {
            expect(partition.readFrom(0)).to.be(false);
        });

        it('returns false when reading beyond file size', function() {
            partition.open();
            partition.write('foobar');
            expect(partition.readFrom(5000)).to.be(false);
        });

        it('can read unflushed documents', function() {
            partition.open();
            let position = partition.write('foobar');
            expect(partition.readFrom(position, 6)).to.be('foobar');
        });

        it('can disable dirty reads', function() {
            partition = new Partition('.part', { dataDirectory: 'test/data', dirtyReads: false });
            partition.open();
            let position = partition.write('foobar');
            expect(partition.readFrom(position, 6)).to.be(false);
        });

        it('validates document size', function() {
            partition.open();
            for (let i = 0; i < 10; i++) {
                partition.write('foobar');
            }
            partition.close();
            partition.open();

            expect(partition.readFrom(0, 6)).to.be('foobar');
            expect(() => partition.readFrom(0, 4)).to.throwError((e) => {
                expect(e).to.be.a(Partition.InvalidDataSizeError);
            });
        });

        it('throws when reading from an invalid position', function() {
            partition.open();
            for (let i = 0; i < 10; i++) {
                partition.write('foobar');
            }
            partition.close();
            partition.open();

            expect(() => console.log(partition.readFrom(4))).to.throwError();
        });

        it('throws when reading unexpected document size', function() {
            partition.open();
            for (let i = 0; i < 10; i++) {
                partition.write('foobar');
            }
            partition.close();
            partition.open();

            expect(() => partition.readFrom(4, 6)).to.throwError();
        });

        it('throws when an unfinished write is found', function() {
            partition.open();
            partition.write('foobar');
            partition.close();

            let fd = fs.openSync('test/data/.part', 'r+');
            let stat = fs.fstatSync(fd);
            fs.ftruncateSync(fd, stat.size - 3);
            fs.closeSync(fd);

            partition.open();

            expect(() => partition.readFrom(0)).to.throwError((e) => {
                expect(e).to.be.a(Partition.CorruptFileError);
            });
        });

        it('can read large documents', function() {
            partition.open();
            let blob = 'foobar'.repeat(100000);
            partition.write(blob);

            partition.close();
            partition.open();

            let read = partition.readFrom(0);
            expect(read).to.be(blob);
        });

    });

    describe('truncate', function() {

        it('does nothing if truncating beyond filesize', function() {
            partition.open();
            let lastposition;
            for (let i = 0; i < 10; i++) {
                lastposition = partition.write('foobar');
            }
            let size = partition.size;
            partition.truncate(lastposition + 1000);
            expect(partition.size).to.be(size);

            partition.close();
            partition.open();
            expect(partition.size).to.be(size);
        });

        it('throws when truncating outside document boundary', function() {
            partition.open();
            let lastposition;
            for (let i = 0; i < 10; i++) {
                lastposition = partition.write('foobar');
            }
            partition.close();
            partition.open();

            expect(() => partition.truncate(lastposition - 3)).to.throwError();
        });

        it('truncates after the given position', function() {
            partition.open();
            let lastposition;
            for (let i = 0; i < 10; i++) {
                lastposition = partition.write('foobar');
            }
            partition.close();
            partition.open();

            partition.truncate(lastposition);
            expect(partition.size).to.be(lastposition);

            partition.close();
            partition.open();
            expect(partition.size).to.be(lastposition);
        });

        it('correctly truncates after unflushed writes', function() {
            partition.open();
            let lastposition;
            for (let i = 0; i < 10; i++) {
                lastposition = partition.write('foobar');
            }
            partition.truncate(lastposition);
            expect(partition.size).to.be(lastposition);

            partition.close();
            partition.open();
            expect(partition.size).to.be(lastposition);
        });

    });

});
