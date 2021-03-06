const expect = require('expect.js');
const fs = require('fs-extra');
const Partition = require('../src/Partition');

const dataDirectory = __dirname + '/data';

describe('Partition', function() {

    /** @type {WritablePartition} */
    let partition;
    /** @type {Array<ReadOnlyPartition>} */
    let readers = [];

    beforeEach(function () {
        fs.emptyDirSync(dataDirectory);
        partition = new Partition('.part', { dataDirectory, readBufferSize: 4*1024 });
    });

    afterEach(function () {
        if (partition) partition.close();
        for (let reader of readers) reader.close();
        readers = [];
        partition = null;
    });

    /**
     * @returns {ReadOnlyPartition}
     */
    function createReader(options = {}) {
        const reader = new Partition.ReadOnly(partition.name, { ...options, dataDirectory });
        readers[readers.length] = reader;
        return reader;
    }

    function fillPartition(num, documentBuilder) {
        let lastPosition;
        for (let i = 1; i <= num; i++) {
            lastPosition = partition.write(documentBuilder && documentBuilder(i) || 'foobar', i);
        }
        partition.flush();
        return lastPosition;
    }

    it('creates the storage directory if it does not exist', function() {
        fs.removeSync(dataDirectory);
        partition = new Partition('.part', { dataDirectory });
        expect(fs.existsSync(dataDirectory)).to.be(true);
    });

    it('is not opened automatically on construct', function() {
        expect(partition.isOpen()).to.be(false);
    });

    it('does nothing on reopening', function() {
        partition.open();
        expect(partition.isOpen()).to.be(true);
        expect(() => partition.open()).to.not.throwError();
    });

    it('throws when not providing partition name', function() {
        expect(() => new Partition()).to.throwError();
        expect(() => new Partition([])).to.throwError();
    });

    it('throws on opening non-partition file', function() {
        let fd = fs.openSync('test/data/.part', 'w');
        fs.writeSync(fd, 'foobar');
        fs.closeSync(fd);

        expect(() => partition.open()).to.throwError();
    });

    it('can open an existing empty file', function() {
        let fd = fs.openSync('test/data/.part', 'w');
        fs.closeSync(fd);

        expect(partition.open()).to.be(true);
    });

    it('throws when mismatching header version', function() {
        let fd = fs.openSync('test/data/.part', 'w');
        fs.writeSync(fd, 'nesprt00');
        fs.closeSync(fd);

        expect(() => partition.open()).to.throwError();
    });

    it('can be opened and closed multiple times', function() {
        partition.open();
        expect(partition.open()).to.be(true);
        partition.close();
        partition.close();
    });

    describe('write', function() {

        it('throws when partition is not open', function() {
            expect(() => partition.write('foo')).to.throwError();
        });

        it('calls callback eventually', function(done) {
            partition.open();
            partition.write('foo', done);
        });

        it('writes data sequentially', function() {
            partition.open();
            fillPartition(50, i => 'foo-' + i.toString());
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

        it('works with small write buffer', function() {
            partition = new Partition('.part', { dataDirectory, writeBufferSize: 64 });
            partition.open();
            fillPartition(50, i => 'foo-' + i.toString());
            partition.close();
            partition.open();
            let i = 1;
            for (let data of partition.readAll()) {
                expect(data).to.be('foo-' + i.toString());
                i++;
            }
        });

    });

    describe('readAll', function() {

        it('reads all documents in write order', function() {
            partition.open();
            fillPartition(50, i => 'foo-' + i.toString());
            partition.close();
            partition.open();
            let i = 1;
            for (let data of partition.readAll()) {
                expect(data).to.be('foo-' + i.toString());
                i++;
            }
            expect(i).to.be(51);
        });

        it('reads all documents in write order from arbitrary position', function() {
            partition.open();
            fillPartition(50, i => 'foo-' + i.toString());
            partition.close();
            partition.open();
            let i = 50;
            for (let data of partition.readAll(-partition.documentWriteSize('foo-50'.length)-1)) {
                expect(data).to.be('foo-' + i.toString());
                i++;
            }
            expect(i).to.be(51);
        });

        it('reads all documents in backwards write order', function() {
            partition.open();
            fillPartition(50, i => 'foo-' + i.toString());
            partition.close();
            partition.open();
            let i = 50;
            for (let data of partition.readAllBackwards()) {
                expect(data).to.be('foo-' + i.toString());
                i--;
            }
            expect(i).to.be(0);
        });

        it('reads all documents in backwards write order from arbitary position', function() {
            partition.open();
            fillPartition(50, i => 'foo-' + i.toString());
            partition.close();
            partition.open();
            let i = 50;
            for (let data of partition.readAllBackwards(-9)) {
                expect(data).to.be('foo-' + i.toString());
                i--;
            }
            expect(i).to.be(0);
            i = 50;
            for (let data of partition.readAllBackwards(partition.size - 12)) {
                expect(data).to.be('foo-' + i.toString());
                i--;
            }
            expect(i).to.be(0);
        });

        it('can find document boundaries by scanning across readbuffers', function() {
            partition.open();
            const lastPosition = fillPartition(3, () => '0xFF'.repeat(64));
            partition.close();

            const reader = createReader({ readBufferSize: 64 });
            reader.open();
            expect(reader.findDocumentPositionBefore(reader.size - 8)).to.be(lastPosition);
            reader.close();
        });

    });

    describe('readFrom', function() {

        it('throws when partition is not open', function() {
            expect(() => partition.readFrom(0)).to.throwError();
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
            partition = new Partition('.part', { dataDirectory, dirtyReads: false });
            partition.open();
            let position = partition.write('foobar');
            expect(partition.readFrom(position, 6)).to.be(false);
        });

        it('validates document size', function() {
            partition.open();
            fillPartition(10);
            partition.close();
            partition.open();

            expect(partition.readFrom(0, 6)).to.be('foobar');
            expect(() => partition.readFrom(0, 4)).to.throwError((e) => {
                expect(e).to.be.a(Partition.InvalidDataSizeError);
            });
        });

        it('throws when reading from an invalid position', function() {
            partition.open();
            fillPartition(10);
            partition.close();
            partition.open();

            expect(() => console.log(partition.readFrom(3))).to.throwError();
        });

        it('throws when reading unexpected document size', function() {
            partition.open();
            fillPartition(10);
            partition.close();
            partition.open();

            expect(() => partition.readFrom(4, 6)).to.throwError();
        });

        it('throws when an unfinished write is found', function() {
            partition.open();
            const position = partition.write('foobar');
            partition.close();

            let fd = fs.openSync('test/data/.part', 'r+');
            fs.ftruncateSync(fd, partition.headerSize + position + partition.documentWriteSize('foobar'.length) - 4);
            fs.closeSync(fd);

            partition.open();

            expect(() => partition.readFrom(0)).to.throwError((e) => {
                expect(e).to.be.a(Partition.CorruptFileError);
            });
        });

        it('can read more documents than fit into a single read buffer', function() {
            partition.open();
            fillPartition(1000);
            partition.close();
            partition.open();

            let pos = 0;
            for (let i = 0; i < 1000; i++) {
                expect(partition.readFrom(pos)).to.be('foobar');
                pos += partition.documentWriteSize('foobar'.length);
            }
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
            let lastposition = fillPartition(10);
            let size = partition.size;
            partition.truncate(lastposition + 1000);
            expect(partition.size).to.be(size);

            partition.close();
            partition.open();
            expect(partition.size).to.be(size);
        });

        it('truncating whole file if negative position given', function() {
            partition.open();
            fillPartition(10);
            partition.close();
            partition.open();

            partition.truncate(-3);
            expect(partition.size).to.be(0);
        });

        it('throws when truncating outside document boundary', function() {
            partition.open();
            let lastposition = fillPartition(10);
            partition.close();
            partition.open();

            expect(() => partition.truncate(lastposition - 3)).to.throwError();
        });

        it('truncates after the given position', function() {
            partition.open();
            let lastposition = fillPartition(10);
            partition.close();
            partition.open();

            partition.truncate(lastposition);
            expect(partition.size).to.be(lastposition);

            partition.close();
            partition.open();
            expect(partition.size).to.be(lastposition);
        });

        it('can not read a truncated document', function() {
            partition.open();
            let lastposition = fillPartition(10);
            expect(partition.readFrom(0)).to.not.be(false);
            expect(partition.readFrom(lastposition)).to.not.be(false);
            partition.truncate(lastposition);
            expect(partition.readFrom(lastposition)).to.be(false);
        });

        it('correctly truncates after unflushed writes', function() {
            partition.open();
            let lastposition = fillPartition(10);
            partition.truncate(lastposition);
            expect(partition.size).to.be(lastposition);

            partition.close();
            partition.open();
            expect(partition.size).to.be(lastposition);
        });

        it('correctly truncates torn writes', function() {
            partition.open();
            const position = fillPartition(5);
            partition.close();

            let fd = fs.openSync('test/data/.part', 'r+');
            fs.ftruncateSync(fd, partition.headerSize + position + Partition.DOCUMENT_HEADER_SIZE + 4);
            fs.closeSync(fd);

            partition.open();

            partition.truncate(position);
            expect(partition.size).to.be(position);
        });

    });

    describe('concurrency', function(){

        it('allows multiple readers for a partition', function(){
            partition.open();
            fillPartition(10);
            expect(partition.size).to.be.greaterThan(0);

            let reader1 = createReader();
            reader1.open();
            expect(reader1.size).to.be(partition.size);
            let reader2 = createReader();
            reader2.open();
            expect(reader2.size).to.be(partition.size);
        });

        it('updates reader when writer appends', function(done){
            partition.open();
            fillPartition(10);
            const size = partition.size;

            let reader = createReader();
            reader.open();
            reader.on('append', (prevSize, newSize) => {
                expect(prevSize).to.be(size);
                expect(newSize).to.be(partition.size);
                done();
            });

            partition.write('foo');
            expect(partition.size).to.be.greaterThan(reader.size);
            partition.flush();
            fs.fdatasync(partition.fd);
        });

        it('updates reader when writer truncates', function(done){
            partition.open();
            fillPartition(10);
            const size = partition.size;

            let reader = createReader();
            reader.open();
            reader.on('truncate', (prevSize, newSize) => {
                expect(prevSize).to.be(size);
                expect(newSize).to.be(partition.size);
                reader.close();
                done();
            });

            partition.truncate(0);
            fs.fdatasync(partition.fd);
        });

        it('recognizes file renames and closes', function(done){
            partition.open();
            fillPartition(10);
            partition.close();

            let reader = createReader();
            reader.open();
            fs.rename(reader.fileName, reader.fileName + '2', () => {
                setTimeout(() => {
                    expect(reader.isOpen()).to.be(false);
                    done();
                }, 5);
            });
        });
    });
});
