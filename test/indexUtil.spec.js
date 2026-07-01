import expect from 'expect.js';
import Entry from '../src/IndexEntry.js';
import {
    intersect,
    union,
    normalizeSelector
} from '../src/utils/indexUtil.js';

function createEntry(number, position = number * 100, size = 10, partition = Math.floor(number / 10)) {
    return new Entry(number, position, size, partition);
}

describe('indexUtil', function() {

    describe('union', function () {

        it('returns empty when called without ranges', function () {
            expect(union()).to.eql([]);
        });

        it('returns a shallow copy for a single range', function () {
            const range = [createEntry(1), createEntry(2)];
            const result = union(range);

            expect(result).to.eql(range);
            expect(result).not.to.be(range);
        });

        it('returns the sorted union of multiple ranges without duplicates', function () {
            const merged = union(
                [createEntry(1), createEntry(3), createEntry(5)],
                [createEntry(2), createEntry(3), createEntry(6)],
                [createEntry(3), createEntry(4), createEntry(6), createEntry(7)]
            );

            expect(merged.map(entry => entry.number)).to.eql([1, 2, 3, 4, 5, 6, 7]);
        });

        it('keeps the left entry when duplicate numbers exist in both ranges', function () {
            const left = createEntry(4, 400, 10, 1);
            const right = createEntry(4, 999, 99, 9);
            const merged = union([left], [right]);

            expect(merged.length).to.be(1);
            expect(merged[0]).to.be(left);
        });

        it('supports chaining with returned arrays', function () {
            const merged = union(
                union([createEntry(1), createEntry(3)], [createEntry(2), createEntry(3)]),
                [createEntry(3), createEntry(4)]
            );

            expect(merged.map(entry => entry.number)).to.eql([1, 2, 3, 4]);
        });

        it('handles empty ranges inside variadic input', function () {
            const merged = union(
                [],
                [createEntry(2), createEntry(4)],
                []
            );

            expect(merged.map(entry => entry.number)).to.eql([2, 4]);
        });

    });

    describe('intersect', function () {

        it('returns empty when called without ranges', function () {
            expect(intersect()).to.eql([]);
        });

        it('returns empty when any range is empty', function () {
            const selected = intersect(
                [createEntry(1), createEntry(2)],
                [],
                [createEntry(1), createEntry(2)]
            );

            expect(selected).to.eql([]);
        });

        it('returns a shallow copy for a single range', function () {
            const range = [createEntry(10), createEntry(20)];
            const result = intersect(range);

            expect(result).to.eql(range);
            expect(result).not.to.be(range);
        });

        it('returns only entries that occur in every range', function () {
            const selected = intersect(
                [createEntry(1), createEntry(3), createEntry(5), createEntry(7)],
                [createEntry(2), createEntry(3), createEntry(5), createEntry(8)],
                [createEntry(3), createEntry(4), createEntry(5), createEntry(9)]
            );

            expect(selected.map(entry => entry.number)).to.eql([3, 5]);
        });

        it('works independent of input order and range sizes', function () {
            const selected = intersect(
                [createEntry(1), createEntry(2), createEntry(3), createEntry(4), createEntry(5), createEntry(6)],
                [createEntry(3), createEntry(4)],
                [createEntry(2), createEntry(3), createEntry(4), createEntry(8)]
            );

            expect(selected.map(entry => entry.number)).to.eql([3, 4]);
        });

        it('returns empty when ranges are disjoint', function () {
            const selected = intersect(
                [createEntry(1), createEntry(2)],
                [createEntry(3), createEntry(4)],
                [createEntry(5), createEntry(6)]
            );

            expect(selected).to.eql([]);
        });

        it('supports chaining merge and select operations', function () {
            const selected = intersect(
                union([createEntry(1), createEntry(4)], [createEntry(2), createEntry(4)]),
                union([createEntry(2), createEntry(4)], [createEntry(3), createEntry(4)]),
                [createEntry(4), createEntry(5)]
            );

            expect(selected.map(entry => entry.number)).to.eql([4]);
        });

    });

    describe('immutability guarantees', function () {

        it('does not mutate source arrays when intersect sorts ranges by length', function () {
            const a = [createEntry(1), createEntry(2), createEntry(3)];
            const b = [createEntry(2)];
            const c = [createEntry(2), createEntry(3)];

            intersect(a, b, c);

            expect(a.map(entry => entry.number)).to.eql([1, 2, 3]);
            expect(b.map(entry => entry.number)).to.eql([2]);
            expect(c.map(entry => entry.number)).to.eql([2, 3]);
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

        it('does not flatten one-level wrapper around OR branch into AND branch', function () {
            const normalized = normalizeSelector(['a', [['x', 'y']], 'b']);
            expect(normalized).to.eql(['a', [['x', 'y']], 'b']);
        });

        it('normalizes [a, [[[x]]], b] to [a, [x], b]', function () {
            const normalized = normalizeSelector(['a', [[['x']]], 'b']);
            expect(normalized).to.eql(['a', ['x'], 'b']);
        });

        it('normalizes [a, [[x]], b] to [a, x, b]', function () {
            const normalized = normalizeSelector(['a', [['x']], 'b']);
            expect(normalized).to.eql(['a', 'x', 'b']);
        });

        function reducer(lengths) {
            return (name) => lengths[name] || 0;
        }

        it('keeps zero-length selectors in the optimized selector', function() {
            const lengths = reducer({ a: 10, b: 0 });
            const normalized = normalizeSelector(['a', 'b'], lengths);

            expect(normalized).to.eql(['a', 'b']);
        });

        it('keeps AND branches with missing streams (for commit-condition stability)', function() {
            const normalized = normalizeSelector([['a', 'missing']], reducer({ a: 10, missing: 0 }));
            expect(normalized).to.eql([['missing', 'a']]);
        });

        it('sorts AND branches by computed child length', function() {
            const normalized = normalizeSelector([['slow', 'fast', 'mid']], reducer({ slow: 100, fast: 1, mid: 10 }));
            expect(normalized).to.eql([['fast', 'mid', 'slow']]);
        });

        it('keeps a single nested AND group intact', function() {
            const normalized = normalizeSelector([['a', 'b']], reducer({ a: 10, b: 3 }));
            expect(normalized).to.eql([['b', 'a']]);
        });

        it('flattens two single-child wrapper levels around a multi-element group', function() {
            const normalized = normalizeSelector([[['a', 'b']]], reducer({ a: 10, b: 3 }));
            expect(normalized).to.eql(['a', 'b']);
        });

        it('does not flatten one-level wrapper around OR branch into AND branch', function() {
            const normalized = normalizeSelector(['a', [['x', 'y']], 'b'], reducer({ a: 10, x: 5, y: 7, b: 3 }));
            expect(normalized).to.eql(['a', [['x', 'y']], 'b']);
        });

        it('normalizes [a, [[[x]]], b] to [a, [x], b]', function() {
            const normalized = normalizeSelector(['a', [[['x']]], 'b'], reducer({ a: 10, x: 5, b: 3 }));
            expect(normalized).to.eql(['a', ['x'], 'b']);
        });

        it('normalizes [a, [[x]], b] to [a, x, b]', function() {
            const normalized = normalizeSelector(['a', [['x']], 'b'], reducer({ a: 10, x: 5, b: 3 }));
            expect(normalized).to.eql(['a', 'x', 'b']);
        });

        it('keeps _all when an AND branch only contains _all', function() {
            const normalized = normalizeSelector([['_all']], reducer({ _all: 100 }));
            expect(normalized).to.eql(['_all']);
        });

    });
});