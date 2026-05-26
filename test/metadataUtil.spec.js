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

        it('throws when matcher is not an object', function() {
            expect(() => buildRawBufferMatcher(null)).to.throwError(TypeError);
            expect(() => buildRawBufferMatcher(['Foo'])).to.throwError(TypeError);
            expect(() => buildRawBufferMatcher('Foo')).to.throwError(TypeError);
        });

        it('throws when an array matcher contains object values', function() {
            expect(() => buildRawBufferMatcher({ type: [{ value: 'Foo' }] })).to.throwError(TypeError);
        });

        it('matches every object when matcher is empty', function() {
            const matcher = buildRawBufferMatcher({});
            expect(matcher(Buffer.from('{"type":"Foo"}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"nested":{"value":1}}', 'utf8'))).to.be(true);
        });

        it('does not match non-object JSON buffers', function() {
            const matcher = buildRawBufferMatcher({ type: 'Foo' });
            expect(matcher(Buffer.from('[{"type":"Foo"}]', 'utf8'))).to.be(false);
        });

        it('does not match when the required nested object key is missing', function() {
            const matcher = buildRawBufferMatcher({ payload: { type: 'Foo' } });
            const buffer = Buffer.from('{"meta":{"type":"Foo"},"id":1}', 'utf8');
            expect(matcher(buffer)).to.be(false);
        });

        it('matches string values containing escaped quotes', function() {
            const matcher = buildRawBufferMatcher({ payload: { text: 'a"b' } });
            const matchingBuffer = Buffer.from('{"payload":{"text":"a\\\"b"}}', 'utf8');
            const nonMatchingBuffer = Buffer.from('{"payload":{"text":"ab"}}', 'utf8');

            expect(matcher(matchingBuffer)).to.be(true);
            expect(matcher(nonMatchingBuffer)).to.be(false);
        });

        it('matches when escaped characters appear before the matched key on the same level', function() {
            const matcher = buildRawBufferMatcher({ type: 'Foo' });
            const buffer = Buffer.from('{"note":"a\\\\b and \\\"quoted\\\"","type":"Foo"}', 'utf8');
            expect(matcher(buffer)).to.be(true);
        });

        it('does not match malformed objects with a premature closing brace before the key', function() {
            const matcher = buildRawBufferMatcher({ type: 'Foo' });
            const buffer = Buffer.from('{"payload":{"id":1}},"type":"Foo"}', 'utf8');
            expect(matcher(buffer)).to.be(false);
        });

        it('does not match nested object keys that are only present deeper than the requested level', function() {
            const matcher = buildRawBufferMatcher({ payload: { source: { kind: 'A' } } });
            const buffer = Buffer.from('{"payload":{"wrapper":{"source":{"kind":"A"}}}}', 'utf8');
            expect(matcher(buffer)).to.be(false);
        });

        it('does not match when a nested object exists but its nested value differs', function() {
            const matcher = buildRawBufferMatcher({ payload: { source: { kind: 'A' } } });
            const buffer = Buffer.from('{"payload":{"source":{"kind":"B"}}}', 'utf8');
            expect(matcher(buffer)).to.be(false);
        });

        it('does not match scalar prefixes like 30 for an expected value of 3', function() {
            const matcher = buildRawBufferMatcher({ version: 3 });
            const buffer = Buffer.from('{"version":30}', 'utf8');
            expect(matcher(buffer)).to.be(false);
        });

    });

});


