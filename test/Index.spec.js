const expect = require('expect.js');
const fs = require('fs-extra');
const Index = require('../src/Index');

describe('index', function() {

    let index;

    beforeEach(function() {
        fs.emptyDirSync('test/data');
    });

    afterEach(function() {
        if (index) index.close();
        index = undefined;
    });

    it('is opened on instanciation', function() {
        index = new Index('test/data/.index');
        expect(index.isOpen()).to.be(true);
    });

    it('recovers metadata on reopening', function() {
        index = new Index('test/data/.index', { metadata: { test: 'valueStays' } });
        expect(index.metadata.test).to.be('valueStays');
        index.close();
        index = new Index('test/data/.index');
        expect(index.metadata.test).to.be('valueStays');
    });

    it('throws on opening an non-index file', function() {
        let fd = fs.openSync('test/data/.index', 'a+');
        fs.writeSync(fd, 'foo');
        fs.closeSync(fd);
        expect(function(){ new Index('test/data/.index') }).to.throwError();
    });

    it('throws on reopening with altered metadata', function() {
        index = new Index('test/data/.index', { metadata: { test: 'valueStays' } });
        expect(function(){ new Index('test/data/.index', { metadata: { test: 'anotherValue' } }) }).to.throwError();
    });

    it('appends entries sequentially', function() {
        index = new Index('test/data/.index');
        for (let i = 1; i <= 100; i++) {
            index.add(new Index.Entry(i, i));
        }
        index.close();
        index.open();
        let entries = index.all();
        expect(entries.length).to.be(100);
        for (let i = 1; i <= entries.length; i++) {
            expect(entries[i - 1].number).to.be(i);
        }
    });

    describe('get', function() {

        it('returns false on out of bounds position', function() {
            index = new Index('test/data/.index');
            for (let i = 1; i <= 50; i++) {
                index.add(new Index.Entry(i, i));
            }
            index.close();
            index.open();
            expect(index.get(0)).to.be(false);
            expect(index.get(51)).to.be(false);
        });

        it('returns false on closed index', function() {
            index = new Index('test/data/.index');
            index.add(new Index.Entry(1, 1));
            index.close();
            expect(index.get(1)).to.be(false);
        });

        it('can random read entries', function() {
            index = new Index('test/data/.index');
            for (let i = 1; i <= 10; i++) {
                index.add(new Index.Entry(i, i));
            }
            index.close();
            index.open();
            let entry = index.get(5);
            expect(entry.number).to.be(5);
        });

    });

    describe('range', function() {

        it('returns false on out of bounds range position', function() {
            index = new Index('test/data/.index');
            for (let i = 1; i <= 50; i++) {
                index.add(new Index.Entry(i, i));
            }
            index.close();
            index.open();
            expect(index.range(0)).to.be(false);
            expect(index.range(51, 55)).to.be(false);
            expect(index.range(1, 51)).to.be(false);
            expect(index.range(15, 10)).to.be(false);
        });

        it('returns false on closed index', function() {
            index = new Index('test/data/.index');
            index.add(new Index.Entry(1, 1));
            index.close();
            expect(index.range(1)).to.be(false);
        });

        it('can read an arbitrary range of entries', function() {
            index = new Index('test/data/.index');
            for (let i = 1; i <= 50; i++) {
                index.add(new Index.Entry(i, i));
            }
            index.close();
            index.open();
            let entries = index.range(21, 37);
            for (let i = 0; i < entries.length; i++) {
                expect(entries[i].number).to.be(21 + i);
            }
        });

        it('can read a range of entries from the end', function() {
            index = new Index('test/data/.index');
            for (let i = 1; i <= 50; i++) {
                index.add(new Index.Entry(i, i));
            }
            index.close();
            index.open();
            let entries = index.range(-15);
            for (let i = 0; i < entries.length; i++) {
                expect(entries[i].number).to.be(50 - 15 + i);
            }
        });

        it('can read a range of entries until a distance from the end', function() {
            index = new Index('test/data/.index');
            for (let i = 1; i <= 50; i++) {
                index.add(new Index.Entry(i, i));
            }
            index.close();
            index.open();
            let entries = index.range(1, -15);
            expect(entries.length).to.be(35);
            for (let i = 0; i < entries.length; i++) {
                expect(entries[i].number).to.be(1 + i);
            }
        });

    });
});
