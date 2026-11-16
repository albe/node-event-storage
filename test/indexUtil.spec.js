import expect from 'expect.js';
import {
    indexRange,
    union,
    intersect,
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
                return numbers.findIndex(value => value >= number) + 1;
            }
            let position = 0;
            numbers.forEach((value, index) => { if (value <= number) position = index + 1; });
            return position;
        }
    };
}

function range(numbers, forwards = true, min = 1, max = 1000) {
    return indexRange(createIndex(numbers), min, max, forwards);
}

function collect(range) {
    return Array.from(range, hit => hit.number);
}

describe('indexUtil', function() {

    describe('indexRange', function () {

        it('yields all numbers within the bounds forwards and backwards', function () {
            expect(collect(range([1, 3, 5, 7, 9], true, 3, 7))).to.eql([3, 5, 7]);
            expect(collect(range([1, 3, 5, 7, 9], false, 2, 8))).to.eql([7, 5, 3]);
        });

        it('is empty for an empty or out of bounds range', function () {
            expect(collect(range([]))).to.eql([]);
            expect(collect(range([5, 6], true, 10, 20))).to.eql([]);
            expect(collect(range([5, 6], false, 1, 4))).to.eql([]);
        });

        it('skips ahead to a target given to next()', function () {
            const numbers = Array.from({ length: 100 }, (_, i) => 2 * (i + 1));
            const forwards = range(numbers);
            forwards.next();
            expect(forwards.next(75).value.number).to.be(76);
            expect(forwards.next(78).value.number).to.be(78);
            expect(forwards.next(201).done).to.be(true);

            const backwards = range(numbers, false);
            backwards.next();
            expect(backwards.next(41).value.number).to.be(40);
            expect(backwards.next(1).done).to.be(true);
        });

        it('yields the index and position of each entry', function () {
            const hits = Array.from(range([2, 4, 6]), hit => hit.position);
            expect(hits).to.eql([1, 2, 3]);
        });
    });

    describe('union', function () {

        it('returns the sorted union without duplicates', function () {
            expect(collect(union(union(range([1, 4, 7]), range([2, 4, 8])), range([])))).to.eql([1, 2, 4, 7, 8]);
        });

        it('returns the union in reverse order when reading backwards', function () {
            expect(collect(union(range([1, 4, 7], false), range([2, 4, 8], false), false))).to.eql([8, 7, 4, 2, 1]);
        });
    });

    describe('intersect', function () {

        it('returns only numbers contained in both ranges', function () {
            const both = intersect(intersect(range([1, 2, 4, 6, 8, 10]), range([2, 3, 4, 8, 11])), range([0, 2, 8, 9]));
            expect(collect(both)).to.eql([2, 8]);
        });

        it('intersects in reverse order when reading backwards', function () {
            expect(collect(intersect(range([1, 2, 4, 8], false), range([2, 4, 5, 8], false), false))).to.eql([8, 4, 2]);
        });

        it('is empty when one range is empty', function () {
            expect(collect(intersect(range([1, 2]), range([])))).to.eql([]);
        });

        it('nests with unions and passes skip targets through them', function () {
            // a AND (b OR c)
            const nested = intersect(range([1, 2, 3, 4, 5, 6, 30]), union(range([2, 9, 30]), range([4, 6])));
            expect(collect(nested)).to.eql([2, 4, 6, 30]);
            // (b OR c) AND a, with long stretches to skip on both sides
            const many = Array.from({ length: 200 }, (_, i) => i + 1);
            const skipping = intersect(union(range([5, 150]), range([100])), range(many));
            expect(collect(skipping)).to.eql([5, 100, 150]);
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