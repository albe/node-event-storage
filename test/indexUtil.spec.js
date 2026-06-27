import expect from 'expect.js';
import Entry from '../src/IndexEntry.js';
import { intersect, union } from '../src/utils/indexUtil.js';

function createEntry(number, position = number * 100, size = 10, partition = Math.floor(number / 10)) {
    return new Entry(number, position, size, partition);
}

describe('indexUtil', function() {

    describe('union', function() {

        it('returns empty when called without ranges', function() {
            expect(union()).to.eql([]);
        });

        it('returns a shallow copy for a single range', function() {
            const range = [createEntry(1), createEntry(2)];
            const result = union(range);

            expect(result).to.eql(range);
            expect(result).not.to.be(range);
        });

        it('returns the sorted union of multiple ranges without duplicates', function() {
            const merged = union(
                [createEntry(1), createEntry(3), createEntry(5)],
                [createEntry(2), createEntry(3), createEntry(6)],
                [createEntry(3), createEntry(4), createEntry(6), createEntry(7)]
            );

            expect(merged.map(entry => entry.number)).to.eql([1, 2, 3, 4, 5, 6, 7]);
        });

        it('keeps the left entry when duplicate numbers exist in both ranges', function() {
            const left = createEntry(4, 400, 10, 1);
            const right = createEntry(4, 999, 99, 9);
            const merged = union([left], [right]);

            expect(merged.length).to.be(1);
            expect(merged[0]).to.be(left);
        });

        it('supports chaining with returned arrays', function() {
            const merged = union(
                union([createEntry(1), createEntry(3)], [createEntry(2), createEntry(3)]),
                [createEntry(3), createEntry(4)]
            );

            expect(merged.map(entry => entry.number)).to.eql([1, 2, 3, 4]);
        });

        it('handles empty ranges inside variadic input', function() {
            const merged = union(
                [],
                [createEntry(2), createEntry(4)],
                []
            );

            expect(merged.map(entry => entry.number)).to.eql([2, 4]);
        });

    });

    describe('intersect', function() {

        it('returns empty when called without ranges', function() {
            expect(intersect()).to.eql([]);
        });

        it('returns empty when any range is empty', function() {
            const selected = intersect(
                [createEntry(1), createEntry(2)],
                [],
                [createEntry(1), createEntry(2)]
            );

            expect(selected).to.eql([]);
        });

        it('returns a shallow copy for a single range', function() {
            const range = [createEntry(10), createEntry(20)];
            const result = intersect(range);

            expect(result).to.eql(range);
            expect(result).not.to.be(range);
        });

        it('returns only entries that occur in every range', function() {
            const selected = intersect(
                [createEntry(1), createEntry(3), createEntry(5), createEntry(7)],
                [createEntry(2), createEntry(3), createEntry(5), createEntry(8)],
                [createEntry(3), createEntry(4), createEntry(5), createEntry(9)]
            );

            expect(selected.map(entry => entry.number)).to.eql([3, 5]);
        });

        it('works independent of input order and range sizes', function() {
            const selected = intersect(
                [createEntry(1), createEntry(2), createEntry(3), createEntry(4), createEntry(5), createEntry(6)],
                [createEntry(3), createEntry(4)],
                [createEntry(2), createEntry(3), createEntry(4), createEntry(8)]
            );

            expect(selected.map(entry => entry.number)).to.eql([3, 4]);
        });

        it('returns empty when ranges are disjoint', function() {
            const selected = intersect(
                [createEntry(1), createEntry(2)],
                [createEntry(3), createEntry(4)],
                [createEntry(5), createEntry(6)]
            );

            expect(selected).to.eql([]);
        });

        it('supports chaining merge and select operations', function() {
            const selected = intersect(
                union([createEntry(1), createEntry(4)], [createEntry(2), createEntry(4)]),
                union([createEntry(2), createEntry(4)], [createEntry(3), createEntry(4)]),
                [createEntry(4), createEntry(5)]
            );

            expect(selected.map(entry => entry.number)).to.eql([4]);
        });

    });

    describe('immutability guarantees', function() {

        it('does not mutate source arrays when intersect sorts ranges by length', function() {
            const a = [createEntry(1), createEntry(2), createEntry(3)];
            const b = [createEntry(2)];
            const c = [createEntry(2), createEntry(3)];

            intersect(a, b, c);

            expect(a.map(entry => entry.number)).to.eql([1, 2, 3]);
            expect(b.map(entry => entry.number)).to.eql([2]);
            expect(c.map(entry => entry.number)).to.eql([2, 3]);
        });

    });

});
