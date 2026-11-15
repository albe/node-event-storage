import expect from 'expect.js';
import {
    EXHAUSTED,
    IndexCursor,
    OrCursor,
    AndCursor,
    normalizeSelector
} from '../src/utils/indexUtil.js';

/**
 * Minimal stand-in for a ReadableIndex over the given ascending sequence numbers.
 */
function createIndex(numbers) {
    return {
        length: numbers.length,
        numberAt: index => numbers[index],
        find(number, min = false) {
            if (min) {
                const position = numbers.findIndex(value => value >= number) + 1;
                return position;
            }
            let position = 0;
            numbers.forEach((value, index) => { if (value <= number) position = index + 1; });
            return position;
        }
    };
}

function cursor(numbers, forwards = true, min = 1, max = 1000) {
    return new IndexCursor(createIndex(numbers), min, max, forwards);
}

function collect(cursor) {
    const keys = [];
    for (; cursor.key !== EXHAUSTED; cursor.next()) {
        keys.push(Math.abs(cursor.key));
    }
    return keys;
}

describe('indexUtil', function() {

    describe('IndexCursor', function () {

        it('walks all numbers within the bounds forwards and backwards', function () {
            expect(collect(cursor([1, 3, 5, 7, 9], true, 3, 7))).to.eql([3, 5, 7]);
            expect(collect(cursor([1, 3, 5, 7, 9], false, 2, 8))).to.eql([7, 5, 3]);
        });

        it('is exhausted for an empty or out of bounds range', function () {
            expect(cursor([], true).key).to.be(EXHAUSTED);
            expect(cursor([5, 6], true, 10, 20).key).to.be(EXHAUSTED);
            expect(cursor([5, 6], false, 1, 4).key).to.be(EXHAUSTED);
        });

        it('seeks to the first key at least the given key and never past the bounds', function () {
            const forwards = cursor([1, 3, 5, 7, 9], true, 1, 7);
            forwards.seek(4);
            expect(forwards.key).to.be(5);
            forwards.seek(2);
            expect(forwards.key).to.be(5);
            forwards.seek(8);
            expect(forwards.key).to.be(EXHAUSTED);

            const backwards = cursor([1, 3, 5, 7, 9], false, 3, 9);
            backwards.seek(-6);
            expect(backwards.key).to.be(-5);
            backwards.seek(-2);
            expect(backwards.key).to.be(EXHAUSTED);
        });

        it('finds far away keys by galloping and binary search', function () {
            const numbers = Array.from({ length: 100 }, (_, i) => 2 * (i + 1));
            const forwards = cursor(numbers, true, 1, 1000);
            forwards.seek(75);
            expect(forwards.key).to.be(76);
            forwards.seek(201);
            expect(forwards.key).to.be(EXHAUSTED);
            const backwards = cursor(numbers, false, 1, 1000);
            backwards.seek(-41);
            expect(backwards.key).to.be(-40);
        });

        it('exposes the index and position of the current entry', function () {
            const indexCursor = cursor([2, 4, 6]);
            indexCursor.next();
            expect(indexCursor.leaf().position).to.be(2);
        });
    });

    describe('OrCursor', function () {

        it('returns the sorted union without duplicates', function () {
            const union = new OrCursor([cursor([1, 4, 7]), cursor([2, 4, 8]), cursor([])]);
            expect(collect(union)).to.eql([1, 2, 4, 7, 8]);
        });

        it('returns the union in reverse order when reading backwards', function () {
            const union = new OrCursor([cursor([1, 4, 7], false), cursor([2, 4, 8], false)]);
            expect(collect(union)).to.eql([8, 7, 4, 2, 1]);
        });

        it('is exhausted without children', function () {
            expect(new OrCursor([]).key).to.be(EXHAUSTED);
        });

        it('points its leaf to a child holding the current key', function () {
            const second = cursor([2, 4]);
            const union = new OrCursor([cursor([3]), second]);
            expect(union.leaf()).to.be(second);
        });
    });

    describe('AndCursor', function () {

        it('returns only keys contained in all children', function () {
            const intersection = new AndCursor([cursor([1, 2, 4, 6, 8, 10]), cursor([2, 3, 4, 8, 11]), cursor([0, 2, 8, 9])]);
            expect(collect(intersection)).to.eql([2, 8]);
        });

        it('intersects in reverse order when reading backwards', function () {
            const intersection = new AndCursor([cursor([1, 2, 4, 8], false), cursor([2, 4, 5, 8], false)]);
            expect(collect(intersection)).to.eql([8, 4, 2]);
        });

        it('is exhausted when a child is empty or there are no children', function () {
            expect(new AndCursor([cursor([1, 2]), cursor([])]).key).to.be(EXHAUSTED);
            expect(new AndCursor([]).key).to.be(EXHAUSTED);
        });

        it('nests with union cursors', function () {
            // (a AND (b OR c))
            const nested = new AndCursor([cursor([1, 2, 3, 4, 5, 6]), new OrCursor([cursor([2, 9]), cursor([4, 6])])]);
            expect(collect(nested)).to.eql([2, 4, 6]);
            const seeking = new OrCursor([new AndCursor([cursor([1, 3, 5]), cursor([3, 5])]), cursor([4])]);
            seeking.seek(4);
            expect(collect(seeking)).to.eql([4, 5]);
        });
    });

    describe('normalizeSelector', function () {

        it('reduces OR branches with _all to _all', function () {
            const normalized = normalizeSelector(['a', '_all', 'b']);
            expect(normalized).to.eql('_all');
        });

        it('keeps single nested _all', function () {
            const normalized = normalizeSelector([['_all']]);
            expect(normalized).to.eql('_all');
        });

        it('keeps a single nested AND group intact', function () {
            const normalized = normalizeSelector([['a', 'b']]);
            expect(normalized).to.eql([['a', 'b']]);
        });

        it('flattens two single-child wrapper levels around a multi-element group', function () {
            const normalized = normalizeSelector([[['a', 'b']]]);
            expect(normalized).to.eql(['a', 'b']);
        });

        it('normalizes [a, [[[x]]], b] to [a, [x], b]', function () {
            const normalized = normalizeSelector(['a', [[['x']]], 'b']);
            expect(normalized).to.eql(['a', ['x'], 'b']);
        });

        it('normalizes [a, [[x]], b] to [a, x, b]', function () {
            const normalized = normalizeSelector(['a', [['x']], 'b']);
            expect(normalized).to.eql(['a', 'x', 'b']);
        });

    });
});