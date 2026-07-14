import expect from 'expect.js';
import {compileDcbQuery, isDcbQuery} from '../src/utils/dcbUtil.js';

const resolveType = (type) => type;
const resolveTag = (tag) => 'tags/' + tag;
const throwOnType = () => {
    throw new Error('typeAccessor not configured');
};
const throwOnTag = () => {
    throw new Error('tagsAccessor not configured');
};

describe('dcbUtil', function () {

    describe('isDcbQuery', function () {

        it('returns true for a plain object with items property', function () {
            expect(isDcbQuery({items: []})).to.be(true);
            expect(isDcbQuery({items: [{types: ['A']}]})).to.be(true);
        });

        it('returns false for arrays', function () {
            expect(isDcbQuery(['A', 'B'])).to.be(false);
            expect(isDcbQuery([])).to.be(false);
        });

        it('returns false for null and primitives', function () {
            expect(isDcbQuery(null)).to.be(false);
            expect(isDcbQuery(undefined)).to.be(false);
            expect(isDcbQuery('string')).to.be(false);
            expect(isDcbQuery(42)).to.be(false);
        });

        it('returns false for objects without an items property', function () {
            expect(isDcbQuery({types: ['A']})).to.be(false);
            expect(isDcbQuery({})).to.be(false);
        });

    });

    describe('compileDcbQuery', function () {

        describe('query-level validation', function () {

            it('throws when items is missing', function () {
                expect(() => compileDcbQuery({}, resolveType, resolveTag)).to.throwError(/items must be a non-empty array/);
            });

            it('throws when items is null', function () {
                expect(() => compileDcbQuery({items: null}, resolveType, resolveTag)).to.throwError(/items must be a non-empty array/);
            });

            it('throws when items is an empty array', function () {
                expect(() => compileDcbQuery({items: []}, resolveType, resolveTag)).to.throwError(/items must be a non-empty array/);
            });

        });

        describe('item-level validation', function () {

            it('throws when an item is null', function () {
                expect(() => compileDcbQuery({items: [null]}, resolveType, resolveTag)).to.throwError(/item must be an object/);
            });

            it('throws when an item is undefined', function () {
                expect(() => compileDcbQuery({items: [undefined]}, resolveType, resolveTag)).to.throwError(/item must be an object/);
            });

            it('throws when item.types is null', function () {
                expect(() => compileDcbQuery({items: [{types: null}]}, resolveType, resolveTag)).to.throwError(/item\.types must be an array/);
            });

            it('throws when item.tags is null', function () {
                expect(() => compileDcbQuery({items: [{tags: null}]}, resolveType, resolveTag)).to.throwError(/item\.tags must be an array/);
            });

            it('throws when item.types is a non-array value', function () {
                expect(() => compileDcbQuery({items: [{types: 'A'}]}, resolveType, resolveTag)).to.throwError(/item\.types must be an array/);
            });

        });

        describe('unconstrained items', function () {

            it('compiles {} to _all', function () {
                expect(compileDcbQuery({items: [{}]}, resolveType, resolveTag)).to.eql(['_all']);
            });

            it('compiles {types: []} to _all', function () {
                expect(compileDcbQuery({items: [{types: []}]}, resolveType, resolveTag)).to.eql(['_all']);
            });

            it('compiles {tags: []} to _all', function () {
                expect(compileDcbQuery({items: [{tags: []}]}, resolveType, resolveTag)).to.eql(['_all']);
            });

            it('compiles {types: [], tags: []} to _all', function () {
                expect(compileDcbQuery({items: [{types: [], tags: []}]}, resolveType, resolveTag)).to.eql(['_all']);
            });

        });

        describe('type-only items', function () {

            it('compiles a single type to a plain string', function () {
                expect(compileDcbQuery({items: [{types: ['A']}]}, resolveType, resolveTag)).to.eql(['A']);
            });

            it('compiles multiple types to a flat array (OR group)', function () {
                expect(compileDcbQuery({items: [{types: ['A', 'B']}]}, resolveType, resolveTag)).to.eql([['A', 'B']]);
            });

        });

        describe('tag-only items', function () {

            it('compiles a single tag to a tags/ prefixed string', function () {
                expect(compileDcbQuery({items: [{tags: ['t1']}]}, resolveType, resolveTag)).to.eql(['tags/t1']);
            });

            it('compiles multiple tags to an AND array with tags/ prefix', function () {
                expect(compileDcbQuery({items: [{tags: ['t1', 't2']}]}, resolveType, resolveTag)).to.eql([['tags/t1', 'tags/t2']]);
            });

        });

        describe('combined tag + type items', function () {

            it('compiles tag + single type to an AND array', function () {
                const result = compileDcbQuery({
                    items: [{
                        tags: ['course:1'],
                        types: ['OrderPlaced']
                    }]
                }, resolveType, resolveTag);
                expect(result).to.eql([['tags/course:1', 'OrderPlaced']]);
            });

            it('compiles tag + multiple types to AND array with nested OR group', function () {
                const result = compileDcbQuery({
                    items: [{
                        tags: ['course:1'],
                        types: ['A', 'B']
                    }]
                }, resolveType, resolveTag);
                expect(result).to.eql([['tags/course:1', ['A', 'B']]]);
            });

            it('compiles multiple tags + multiple types correctly', function () {
                const result = compileDcbQuery({
                    items: [{
                        tags: ['t1', 't2'],
                        types: ['A', 'B']
                    }]
                }, resolveType, resolveTag);
                expect(result).to.eql([['tags/t1', 'tags/t2', ['A', 'B']]]);
            });

        });

        describe('multi-item queries', function () {

            it('wraps multiple items in a top-level OR array', function () {
                const result = compileDcbQuery({
                    items: [
                        {tags: ['course:1'], types: ['CourseCreated', 'CourseCapacityChanged']},
                        {tags: ['student:2'], types: ['StudentCreated']}
                    ]
                }, resolveType, resolveTag);
                expect(result).to.eql([
                    ['tags/course:1', ['CourseCreated', 'CourseCapacityChanged']],
                    ['tags/student:2', 'StudentCreated']
                ]);
            });

        });

        describe('resolver delegation', function () {

            it('propagates errors from resolveType', function () {
                expect(() => compileDcbQuery({items: [{types: ['A']}]}, throwOnType, resolveTag))
                    .to.throwError(/typeAccessor not configured/);
            });

            it('propagates errors from resolveTag', function () {
                expect(() => compileDcbQuery({items: [{tags: ['t1']}]}, resolveType, throwOnTag))
                    .to.throwError(/tagsAccessor not configured/);
            });

        });

    });

});
