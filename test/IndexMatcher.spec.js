import expect from 'expect.js';
import IndexMatcher from '../src/IndexMatcher.js';

describe('IndexMatcher', function() {

    describe('remove', function() {

        it('is a no-op for unknown index names', function() {
            const matcher = new IndexMatcher(['stream']);
            expect(() => matcher.remove('missing')).to.not.throwError();
        });

        it('removes function matchers from the function matcher set', function() {
            const matcher = new IndexMatcher(['stream']);
            const fn = (doc) => doc.stream === 'orders';
            matcher.add('orders-fn', fn);
            expect(matcher.functionMatchers.has('orders-fn')).to.be(true);

            matcher.remove('orders-fn');

            expect(matcher.functionMatchers.has('orders-fn')).to.be(false);
            expect(matcher.matchers.has('orders-fn')).to.be(false);
        });

        it('removes non-object matchers from the unclassified set', function() {
            const matcher = new IndexMatcher(['stream']);
            matcher.add('invalid-matcher', true);
            expect(matcher.unclassifiedMatchers.has('invalid-matcher')).to.be(true);

            matcher.remove('invalid-matcher');

            expect(matcher.unclassifiedMatchers.has('invalid-matcher')).to.be(false);
            expect(matcher.matchers.has('invalid-matcher')).to.be(false);
        });

        it('removes object matchers without discriminants from the unclassified set', function() {
            const matcher = new IndexMatcher(['stream']);
            matcher.add('type-only', { type: 'OrderPlaced' });
            expect(matcher.unclassifiedMatchers.has('type-only')).to.be(true);

            matcher.remove('type-only');

            expect(matcher.unclassifiedMatchers.has('type-only')).to.be(false);
            expect(matcher.matchers.has('type-only')).to.be(false);
        });

    });

});
