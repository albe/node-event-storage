import expect from 'expect.js';
import fs from 'fs-extra';
import path from 'path';
import InvertedIndex, { CorruptSegmentError } from '../src/InvertedIndex.js';
import { ByteWriter, ByteReader, crc32, crc32Fallback } from '../src/InvertedIndex/encoding.js';
import { fileURLToPath } from 'url';

const __dirname = fileURLToPath(new URL('.', import.meta.url));
const dataDirectory = __dirname + 'data';

describe('InvertedIndex', function() {

    let index;

    beforeEach(function() {
        fs.emptyDirSync(dataDirectory);
    });

    afterEach(function() {
        if (index) index.close();
        index = null;
    });

    function get(term, from) {
        return Array.from(index.range(term, from), hit => hit.number);
    }

    function query(selector, from) {
        return Array.from(index.query(selector, from), hit => hit.number);
    }

    function createIndex(options = {}) {
        index = new InvertedIndex('test', { dataDirectory, syncOnFlush: false, ...options });
        index.open();
        return index;
    }

    function reopen(options = {}) {
        index.close();
        return createIndex(options);
    }

    function segmentFiles() {
        return fs.readdirSync(dataDirectory).filter(file => file.endsWith('.inv'));
    }

    describe('encoding', function() {

        it('round-trips varints up to 2^53', function() {
            const values = [0, 1, 127, 128, 16383, 16384, 2 ** 31 - 1, 2 ** 31, 2 ** 32 + 5, Number.MAX_SAFE_INTEGER];
            const writer = new ByteWriter(1);
            values.forEach(value => writer.varint(value));
            const reader = new ByteReader(writer.toBuffer());
            expect(values.map(() => reader.varint())).to.eql(values);
        });

        it('computes the same crc32 as the native implementation', function() {
            const buffer = Buffer.from('event-storage inverted index');
            expect(crc32Fallback(buffer)).to.be(crc32(buffer));
            expect(crc32(Buffer.from('123456789'))).to.be(0xCBF43926);
        });
    });

    it('returns positions per term from the memtable', function() {
        createIndex();
        index.add(1, ['type:A', 'tag:x']);
        index.add(2, ['type:B']);
        index.add(5, ['type:A']);
        expect(get('type:A')).to.eql([1, 5]);
        expect(get('type:A', 2)).to.eql([5]);
        expect(get('unknown')).to.eql([]);
        expect(index.count('type:A')).to.be(2);
        expect(index.lastPosition('type:A')).to.be(5);
        expect(index.length).to.be(5);
    });

    it('ignores a term repeated for the same position', function() {
        createIndex();
        index.add(1, ['tag:x', 'tag:x']);
        expect(get('tag:x')).to.eql([1]);
    });

    it('rejects positions that are not ascending', function() {
        createIndex();
        index.add(3, ['a']);
        expect(() => index.add(3, ['a'])).to.throwError(/ascending/);
        expect(() => index.add(2, ['a'])).to.throwError(/ascending/);
        expect(() => index.add(1.5, ['a'])).to.throwError(/ascending/);
    });

    it('persists postings in a segment on close and reads them after reopening', function() {
        createIndex();
        for (let position = 1; position <= 1000; position++) {
            index.add(position, ['all', position % 2 ? 'odd' : 'even', 'mod7:' + (position % 7)]);
        }
        reopen();
        expect(index.stats().segments).to.be(1);
        expect(index.stats().persistedUntil).to.be(1000);
        expect(index.count('all')).to.be(1000);
        expect(get('odd').length).to.be(500);
        expect(get('mod7:3', 990)).to.eql([990, 997]);
        expect(index.lastPosition('even')).to.be(1000);
        expect(index.verify()).to.be(true);
    });

    it('combines segments and memtable in ascending order', function() {
        createIndex({ flushThreshold: 10, mergeFactor: 100 });
        for (let position = 1; position <= 95; position++) {
            index.add(position, ['all']);
        }
        expect(index.stats().segments).to.be(9);
        expect(get('all')).to.eql(Array.from({ length: 95 }, (_, i) => i + 1));
        expect(get('all', 50).length).to.be(46);
    });

    it('uses the skip table to start in the middle of a long posting list', function() {
        createIndex();
        for (let position = 1; position <= 10000; position++) {
            index.add(position * 3, ['all']);
        }
        reopen();
        expect(get('all', 29998)).to.eql([29999, 30000].filter(p => p % 3 === 0));
        expect(get('all', 15000).length).to.be(5001);
        expect(get('all', 1)[0]).to.be(3);
    });

    it('supports positions beyond 32 bit', function() {
        createIndex();
        const big = 2 ** 40;
        index.add(big, ['a']);
        index.add(big + 1, ['a']);
        reopen();
        expect(get('a')).to.eql([big, big + 1]);
    });

    it('reports the persisted position so unflushed documents can be re-added', function() {
        createIndex({ flushThreshold: 4 });
        index.add(1, ['a', 'b']);
        index.add(2, ['a', 'b']);
        index.add(3, ['a']);
        const crashed = index;
        index = new InvertedIndex('test', { dataDirectory, syncOnFlush: false });
        expect(index.open()).to.be(2);
        expect(get('a')).to.eql([1, 2]);
        index.add(3, ['a']);
        expect(get('a')).to.eql([1, 2, 3]);
        crashed.segments.forEach(segment => segment.close());
    });

    it('removes segment files that are not in the manifest', function() {
        createIndex();
        index.add(1, ['a']);
        reopen();
        fs.writeFileSync(path.join(dataDirectory, 'test.seg-000099.inv'), 'half written');
        reopen();
        expect(segmentFiles()).to.eql(['test.seg-000001.inv']);
    });

    it('persists positions without terms', function() {
        createIndex();
        index.add(1, []);
        index.add(2, []);
        reopen();
        expect(index.stats().persistedUntil).to.be(2);
        expect(index.stats().segments).to.be(0);
    });

    it('merges adjacent segments when there are too many', function() {
        createIndex({ flushThreshold: 5, mergeFactor: 3 });
        const expected = { a: [], b: [] };
        for (let position = 1; position <= 200; position++) {
            const term = position % 3 ? 'a' : 'b';
            index.add(position, [term, 'tag:' + (position % 10)]);
            expected[term].push(position);
        }
        expect(index.stats().segments).to.be.below(4);
        expect(get('a')).to.eql(expected.a);
        expect(get('b')).to.eql(expected.b);
        expect(get('tag:7').length).to.be(20);
        reopen({ flushThreshold: 5, mergeFactor: 3 });
        expect(get('a')).to.eql(expected.a);
        expect(segmentFiles().length).to.be(index.stats().segments);
        expect(index.verify()).to.be(true);
    });

    it('evaluates the selector algebra with OR at even and AND at odd depth', function() {
        createIndex({ flushThreshold: 7 });
        const events = [
            ['type:CourseDefined', 'tag:course/1'],
            ['type:StudentRegistered', 'tag:student/1'],
            ['type:StudentSubscribed', 'tag:course/1', 'tag:student/1'],
            ['type:CourseDefined', 'tag:course/2'],
            ['type:StudentSubscribed', 'tag:course/2', 'tag:student/1'],
            ['type:CourseCapacityChanged', 'tag:course/1']
        ];
        events.forEach((terms, i) => index.add(i + 1, terms));

        expect(query(['type:CourseDefined', 'type:StudentRegistered'])).to.eql([1, 2, 4]);
        expect(query([['tag:course/1', 'tag:student/1']])).to.eql([3]);
        expect(query([
            ['tag:course/1', ['type:CourseDefined', 'type:CourseCapacityChanged']],
            ['tag:student/1', 'type:StudentSubscribed']
        ])).to.eql([1, 3, 5, 6]);
        expect(query('tag:course/2', 5)).to.eql([5]);
    });

    it('skips ahead when next() is called with a target position', function() {
        createIndex({ flushThreshold: 300 });
        for (let position = 1; position <= 1000; position++) {
            index.add(position, position % 2 === 0 ? ['even'] : ['odd']);
        }
        const range = index.range('even');
        expect(range.next().value.number).to.be(2);
        expect(range.next(551).value.number).to.be(552);
        expect(range.next(553).value.number).to.be(554);
        expect(range.next(999).value.number).to.be(1000);
        expect(range.next().done).to.be(true);
        expect(get('odd', 899).slice(0, 2)).to.eql([899, 901]);
    });

    it('checks DCB conditions only against positions after the given one', function() {
        createIndex({ flushThreshold: 3 });
        index.add(1, ['type:A', 'tag:x']);
        index.add(2, ['type:B', 'tag:y']);
        const selector = [['type:A', 'tag:x']];
        expect(index.hasMatchAfter(selector, 2)).to.be(false);
        index.add(3, ['type:A', 'tag:y']);
        expect(index.hasMatchAfter(selector, 2)).to.be(false);
        index.add(4, ['type:A', 'tag:x']);
        expect(index.hasMatchAfter(selector, 2)).to.be(true);
        expect(index.hasMatchAfter(selector, 4)).to.be(false);
        expect(index.hasMatchAfter('tag:y', 1)).to.be(true);
    });

    it('matches a brute-force evaluation on random data across segments, merges and memtable', function() {
        createIndex({ flushThreshold: 300, mergeFactor: 3 });
        let seed = 42;
        const random = (n) => (seed = (seed * 1103515245 + 12345) % 2147483648) % n;
        const docs = [];
        let position = 0;
        for (let i = 0; i < 3000; i++) {
            position += 1 + random(3);
            const terms = ['type:' + random(4), 'tag:a/' + random(20)];
            if (random(2)) terms.push('tag:b/' + random(50));
            index.add(position, terms);
            docs.push({ position, terms: new Set(terms) });
        }
        const matches = (node, terms, depth = 0) => typeof node === 'string'
            ? terms.has(node)
            : (depth % 2 === 0 ? node.some(c => matches(c, terms, depth + 1)) : node.every(c => matches(c, terms, depth + 1)));
        const selectors = [
            ['type:1'],
            [['tag:a/3', 'type:2']],
            [['tag:a/3', ['type:1', 'type:2']], ['tag:b/7', 'type:0'], 'tag:a/19'],
            [['tag:a/1', 'tag:b/1', 'type:3']]
        ];
        for (const selector of selectors) {
            for (const from of [1, 500, 4000]) {
                const expected = docs.filter(d => d.position >= from && matches(selector, d.terms)).map(d => d.position);
                expect(query(selector, from)).to.eql(expected);
                expect(index.hasMatchAfter(selector, from - 1)).to.be(expected.length > 0);
            }
        }
    });

    it('lists terms by prefix across segments and memtable', function() {
        createIndex({ flushThreshold: 2 });
        index.add(1, ['tag:course/1', 'tag:student/1']);
        index.add(2, ['tag:course/2']);
        index.add(3, ['tag:course/3', 'type:X']);
        expect(index.terms('tag:course/')).to.eql(['tag:course/1', 'tag:course/2', 'tag:course/3']);
        expect(index.terms('tag:z')).to.eql([]);
        expect(index.terms().length).to.be(5);
    });

    it('handles many terms across dictionary blocks, including non-ASCII terms', function() {
        createIndex();
        const terms = [];
        for (let i = 0; i < 500; i++) terms.push('tag:entity/' + i);
        terms.push('tag:straße', 'tag:日本', 'tag:😀');
        terms.forEach((term, i) => index.add(i + 1, [term]));
        reopen();
        terms.forEach((term, i) => expect(get(term)).to.eql([i + 1]));
        expect(get('tag:entity/5000')).to.eql([]);
        expect(index.terms('tag:entity/49').length).to.be(11);
    });

    it('detects a corrupted segment', function() {
        createIndex();
        for (let position = 1; position <= 100; position++) index.add(position, ['a']);
        index.close();
        const file = path.join(dataDirectory, segmentFiles()[0]);
        const data = fs.readFileSync(file);
        data[20] ^= 0xFF;
        fs.writeFileSync(file, data);
        createIndex();
        expect(index.verify()).to.be(false);
    });

    it('refuses to open a segment with an invalid footer', function() {
        createIndex();
        index.add(1, ['a']);
        index.close();
        const file = path.join(dataDirectory, segmentFiles()[0]);
        fs.truncateSync(file, fs.statSync(file).size - 1);
        index = new InvertedIndex('test', { dataDirectory });
        expect(() => index.open()).to.throwError(e => expect(e).to.be.a(CorruptSegmentError));
        index = null;
    });
});
