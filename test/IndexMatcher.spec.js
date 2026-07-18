import expect from 'expect.js';
import IndexMatcher from '../src/IndexMatcher.js';

describe('IndexMatcher', function() {

    describe('forEachMatch with array document properties', function() {

        it('matches an index when document array contains the discriminant value', function() {
            const im = new IndexMatcher(['payload.tags']);
            im.add('stream-tags/course:1', { payload: { tags: { $has: 'course:1' } } });

            const hits = [];
            im.forEachMatch({ payload: { tags: ['course:1', 'student:9'] } }, n => hits.push(n));
            expect(hits).to.eql(['stream-tags/course:1']);
        });

        it('does not match when the discriminant value is absent from the array', function() {
            const im = new IndexMatcher(['payload.tags']);
            im.add('stream-tags/course:1', { payload: { tags: { $has: 'course:1' } } });

            const hits = [];
            im.forEachMatch({ payload: { tags: ['student:9'] } }, n => hits.push(n));
            expect(hits).to.eql([]);
        });

        it('resolves multiple indexes from multiple array elements', function() {
            const im = new IndexMatcher(['payload.tags']);
            im.add('stream-tags/course:1', { payload: { tags: { $has: 'course:1' } } });
            im.add('stream-tags/student:9', { payload: { tags: { $has: 'student:9' } } });

            const hits = [];
            im.forEachMatch({ payload: { tags: ['course:1', 'student:9'] } }, n => hits.push(n));
            expect(hits.sort()).to.eql(['stream-tags/course:1', 'stream-tags/student:9']);
        });

        it('deduplicates when duplicate array elements map to the same index', function() {
            const im = new IndexMatcher(['payload.tags']);
            im.add('stream-tags/course:1', { payload: { tags: { $has: 'course:1' } } });

            const hits = [];
            im.forEachMatch({ payload: { tags: ['course:1', 'course:1'] } }, n => hits.push(n));
            expect(hits).to.eql(['stream-tags/course:1']);
        });

        it('ignores non-scalar array elements (objects, nulls)', function() {
            const im = new IndexMatcher(['payload.tags']);
            im.add('stream-tags/course:1', { payload: { tags: { $has: 'course:1' } } });

            const hits = [];
            im.forEachMatch({ payload: { tags: [null, {nested: true}, 'course:1'] } }, n => hits.push(n));
            expect(hits).to.eql(['stream-tags/course:1']);
        });

    });

    describe('findDiscriminant $has handling', function() {

        it('does not treat a matcher with a non-$has operator as a $has discriminant', function() {
            const im = new IndexMatcher(['payload.tags']);
            // Non-$has operator falls through: no discriminant, stays unclassified.
            im.add('tag-ne', { payload: { tags: { $ne: 'x' } } });
            expect(im.unclassifiedMatchers.has('tag-ne')).to.be(true);
        });

        it('does not treat an object with multiple keys as a $has discriminant', function() {
            const im = new IndexMatcher(['payload.tags']);
            im.add('tag-mixed', { payload: { tags: { $has: 'x', $ne: 'y' } } });
            expect(im.unclassifiedMatchers.has('tag-mixed')).to.be(true);
        });

        it('does not treat an empty array discriminant value as a $has scalar', function() {
            const im = new IndexMatcher(['payload.tags']);
            // Empty array: not a valid array discriminant (length 0) and Array.isArray is
            // guarded inside isLoneHasScalar, so this falls through to unclassified.
            im.add('tag-empty', { payload: { tags: [] } });
            expect(im.unclassifiedMatchers.has('tag-empty')).to.be(true);
        });

        it('does not treat an array with non-scalar elements as a $has scalar', function() {
            const im = new IndexMatcher(['payload.tags']);
            // Array containing an object: fails the "all scalars" branch, then falls into
            // isLoneHasScalar which rejects arrays outright.
            im.add('tag-mixed-arr', { payload: { tags: [{ nested: true }] } });
            expect(im.unclassifiedMatchers.has('tag-mixed-arr')).to.be(true);
        });

    });

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
