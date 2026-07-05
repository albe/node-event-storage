import expect from 'expect.js';
import { anyOf, allOf, without, not, all, compileSelector } from '../src/Selector.js';

describe('Selector DSL', function() {

    describe('anyOf', function() {

        it('returns a flat OR array for plain streams', function() {
            expect(compileSelector(anyOf('a', 'b', 'c'))).to.eql(['a', 'b', 'c']);
        });

        it('preserves nesting parity: inner anyOf at odd depth gets wrapped', function() {
            // Inner anyOf('b','c') lands at depth 1 (AND context) => gets wrapped to [['b','c']]
            // outer OR at depth 0: ['a', [['b','c']]]
            expect(compileSelector(anyOf('a', anyOf('b', 'c')))).to.eql(['a', [['b', 'c']]]);
        });

        it('applies without() to the immediately preceding argument', function() {
            // anyOf(a, without(b), c) = (a \ b) || c
            expect(compileSelector(anyOf('a', without('b'), 'c'))).to.eql([
                { difference: ['a', 'b'] },
                'c'
            ]);
        });

        it('anyOf(a, b, without(c)) = (a OR b) \\ c', function() {
            // All preceding items (a, b) become source, joined by OR
            expect(compileSelector(anyOf('a', 'b', without('c')))).to.eql([
                { difference: [['a', 'b'], 'c'] }
            ]);
        });

        it('unions multiple excluded streams for single without()', function() {
            // anyOf(a, without(b, c)) = a \ (b || c)
            expect(compileSelector(anyOf('a', without('b', 'c')))).to.eql([
                { difference: ['a', ['b', 'c']] }
            ]);
        });

        it('throws when without() has no preceding argument', function() {
            expect(() => anyOf(without('b'))).to.throwError();
        });

    });

    describe('allOf', function() {

        it('wraps in one extra array level for AND semantics', function() {
            expect(compileSelector(allOf('a', 'b'))).to.eql([['a', 'b']]);
        });

        it('allOf(a, b, without(c)) = (a AND b) \\ c', function() {
            // All preceding items (a, b) become the source, joined by AND
            expect(compileSelector(allOf('a', 'b', without('c')))).to.eql([
                { difference: [[['a', 'b']], 'c'] }
            ]);
        });

        it('allOf(a, b, without(c, d)) = (a AND b) \\ (c AND d)', function() {
            // Multiple without args joined with parent operator (AND)
            expect(compileSelector(allOf('a', 'b', without('c', 'd')))).to.eql([
                { difference: [[['a', 'b']], [['c', 'd']]] }
            ]);
        });

        it('allOf(a, b, without(anyOf(c, d))) = (a AND b) \\ (c OR d)', function() {
            // Explicit anyOf inside without overrides parent operator
            expect(compileSelector(allOf('a', 'b', without(anyOf('c', 'd'))))).to.eql([
                { difference: [[['a', 'b']], ['c', 'd']] }
            ]);
        });

        it('allOf(a, without(b), c) = (a \\ b) AND c', function() {
            // without in the middle: only 'a' is the source, 'c' continues the AND
            expect(compileSelector(allOf('a', without('b'), 'c'))).to.eql([
                [{ difference: ['a', 'b'] }, 'c']
            ]);
        });

    });

    describe('not', function() {

        it('compiles not(a) to _all \\ a', function() {
            expect(compileSelector(not('a'))).to.eql([{ difference: ['_all', 'a'] }]);
        });

        it('compiles not(a, b) to _all \\ (a || b)', function() {
            expect(compileSelector(not('a', 'b'))).to.eql([
                { difference: ['_all', ['a', 'b']] }
            ]);
        });

        it('anyOf(all(), without(b)) is equivalent to not(b)', function() {
            expect(compileSelector(anyOf(all(), without('b')))).to.eql(
                compileSelector(not('b'))
            );
        });

    });

    describe('nested operators', function() {

        it('anyOf(allOf(a, b), c) = (a AND b) OR c', function() {
            expect(compileSelector(anyOf(allOf('a', 'b'), 'c'))).to.eql([
                ['a', 'b'],
                'c'
            ]);
        });

        it('allOf(anyOf(a, b), c) = (a OR b) AND c', function() {
            expect(compileSelector(allOf(anyOf('a', 'b'), 'c'))).to.eql([
                [['a', 'b'], 'c']
            ]);
        });

        it('anyOf(allOf(a, b), without(c)) = (a AND b) \\ c', function() {
            // anyOf with an allOf as source, excluded by c
            expect(compileSelector(anyOf(allOf('a', 'b'), without('c')))).to.eql([
                { difference: [[['a', 'b']], 'c'] }
            ]);
        });

        it('allOf(a, not(b)) = a AND (_all \\ b)', function() {
            // not(b) is itself a difference node; allOf combines it with a via AND
            expect(compileSelector(allOf('a', not('b')))).to.eql([
                ['a', { difference: ['_all', 'b'] }]
            ]);
        });

    });

    describe('single-argument edge cases', function() {

        it('anyOf with single stream', function() {
            expect(compileSelector(anyOf('a'))).to.eql(['a']);
        });

        it('allOf with single stream', function() {
            expect(compileSelector(allOf('a'))).to.eql(['a']);
        });

        it('anyOf with single without at end', function() {
            expect(compileSelector(anyOf('a', without('b')))).to.eql([
                { difference: ['a', 'b'] }
            ]);
        });

    });

});











