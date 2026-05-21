import expect from 'expect.js';
import { buildRawBufferMatcher } from '../src/metadataUtil.js';

describe('metadataUtil', function() {

    describe('buildRawBufferMatcher', function() {

        it('matches top-level scalar properties in raw JSON buffers', function() {
            const matchesTypeFoo = buildRawBufferMatcher({ type: 'Foo' });
            const buffer = Buffer.from('{"type":"Foo","id":1}', 'utf8');
            expect(matchesTypeFoo(buffer)).to.be(true);
        });

        it('does not match nested values with the same key/value', function() {
            const matchesTypeFoo = buildRawBufferMatcher({ type: 'Foo' });
            const buffer = Buffer.from('{"payload":{"type":"Foo"},"type":"Bar"}', 'utf8');
            expect(matchesTypeFoo(buffer)).to.be(false);
        });

        it('supports matchers with multiple top-level properties', function() {
            const matcher = buildRawBufferMatcher({ type: 'Foo', version: 3 });
            const buffer = Buffer.from('{"type":"Foo","version":3,"payload":"ok"}', 'utf8');
            expect(matcher(buffer)).to.be(true);
        });

        it('requires compact key-value separators without whitespace', function() {
            const matcher = buildRawBufferMatcher({ type: 'Foo', enabled: true });
            const buffer = Buffer.from('{ "type" : "Foo", "enabled" : true }', 'utf8');
            expect(matcher(buffer)).to.be(false);
        });

        it('supports nested matcher objects via the scoped fallback', function() {
            const matcher = buildRawBufferMatcher({ payload: { type: 'Foo' } });
            const buffer = Buffer.from('{"payload":{"type":"Foo"},"id":1}', 'utf8');
            expect(matcher(buffer)).to.be(true);
        });

    });

    describe('buildRawBufferMatcher (nested/object-array mode)', function() {

        it('matches one-level nested object properties', function() {
            const matcher = buildRawBufferMatcher({ payload: { type: 'Foo' } });
            const buffer = Buffer.from('{"payload":{"type":"Foo"},"id":1}', 'utf8');
            expect(matcher(buffer)).to.be(true);
        });

        it('does not match when nested value differs', function() {
            const matcher = buildRawBufferMatcher({ payload: { type: 'Foo' } });
            const buffer = Buffer.from('{"payload":{"type":"Bar"},"id":1}', 'utf8');
            expect(matcher(buffer)).to.be(false);
        });

        it('matches one of two allowed nested scalar values', function() {
            const matcher = buildRawBufferMatcher({ payload: { type: ['Foo', 'Bar'] } });
            const fooBuffer = Buffer.from('{"payload":{"type":"Foo"},"id":1}', 'utf8');
            const barBuffer = Buffer.from('{"payload":{"type":"Bar"},"id":2}', 'utf8');
            const bazBuffer = Buffer.from('{"payload":{"type":"Baz"},"id":3}', 'utf8');

            expect(matcher(fooBuffer)).to.be(true);
            expect(matcher(barBuffer)).to.be(true);
            expect(matcher(bazBuffer)).to.be(false);
        });

    });

});


