import expect from 'expect.js';
import {
    excludeStreams,
    union,
    intersect,
    normalizeSelector
} from '../src/utils/indexUtil.js';

/**
 * Stand-in for index.range() over the given ascending sequence numbers, including skipping with next(number).
 */
function* range(numbers, forwards = true, min = 1, max = 1000) {
    const selected = numbers.filter(number => number >= min && number <= max);
    if (!forwards) {
        selected.reverse();
    }
    const before = forwards ? (a, b) => a < b : (a, b) => a > b;
    for (let i = 0; i < selected.length; i++) {
        const target = yield { number: selected[i] };
        while (target !== undefined && i + 1 < selected.length && before(selected[i + 1], target)) {
            i++;
        }
    }
}

function collect(range) {
    return Array.from(range, entry => entry.number);
}

describe('indexUtil', function() {

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

        it('asks the lagging range to skip ahead', function () {
            const targets = [];
            function* recording() {
                for (const number of [1, 2, 3, 4, 5, 6, 7, 8]) {
                    targets.push(yield { number });
                }
            }
            expect(collect(intersect(recording(), range([7, 8])))).to.eql([7, 8]);
            expect(targets[0]).to.be(7);
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

    describe('excludeStreams', function () {
        const without = (selector, ...names) => excludeStreams(selector, new Set(names));

        it('removes streams from an OR level', function () {
            expect(without(['a', 'b', 'c'], 'b')).to.eql(['a', 'c']);
        });

        it('removes an AND level that contains an excluded stream', function () {
            expect(without([['tags/x', 'a'], 'b'], 'a')).to.eql(['b']);
            expect(without([['tags/x', 'a'], 'b'], 'tags/x')).to.eql(['b']);
        });

        it('removes streams from a nested OR inside an AND', function () {
            expect(without([['tags/x', ['a', 'b']]], 'a')).to.eql([['tags/x', ['b']]]);
            expect(without([['tags/x', ['a', 'b']]], 'a', 'b')).to.be(null);
        });

        it('returns null when nothing is left', function () {
            expect(without(['a'], 'a')).to.be(null);
            expect(without('a', 'a')).to.be(null);
        });

        it('never removes _all', function () {
            expect(without(['_all'], '_all')).to.eql(['_all']);
        });

        it('leaves the input unchanged', function () {
            const selector = [['tags/x', ['a', 'b']], 'c'];
            without(selector, 'a', 'c');
            expect(selector).to.eql([['tags/x', ['a', 'b']], 'c']);
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