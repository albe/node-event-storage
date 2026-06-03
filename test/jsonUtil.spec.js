import expect from 'expect.js';
import { indexOfSameLevel, findJsonValueEnd, parseJsonValue } from '../src/utils/jsonUtil.js';

describe('jsonUtil', function() {

    describe('indexOfSameLevel', function() {

        it('finds a top-level value pattern while skipping nested strings and objects', function() {
            const buffer = Buffer.from('{"note":"a\\"b","payload":{"type":"Foo"},"type":"Foo"}', 'utf8');
            const pattern = Buffer.from('"type":"Foo"', 'utf8');

            expect(indexOfSameLevel(buffer, pattern, 1)).to.be(buffer.lastIndexOf(pattern));
        });

        it('matches key patterns without requiring a trailing delimiter check', function() {
            const buffer = Buffer.from('{"payload":{"type":"Foo"},"type":"Bar"}', 'utf8');
            const pattern = Buffer.from('"type":', 'utf8');

            expect(indexOfSameLevel(buffer, pattern, 1, undefined, true)).to.be(buffer.lastIndexOf(pattern));
        });

        it('matches object patterns that end with an opening brace', function() {
            const buffer = Buffer.from('{"payload":{"type":"Foo"}}', 'utf8');
            const pattern = Buffer.from('"payload":{', 'utf8');

            expect(indexOfSameLevel(buffer, pattern, 1)).to.be(buffer.indexOf(pattern));
        });

        it('returns -1 when the pattern never appears at the requested level', function() {
            const buffer = Buffer.from('{"type":"Foo","payload":{"kind":"A"}}', 'utf8');
            const pattern = Buffer.from('"missing":"value"', 'utf8');

            expect(indexOfSameLevel(buffer, pattern, 1, -1)).to.be(-1);
        });

    });

    describe('findJsonValueEnd', function() {

        it('returns the end of a quoted string value including escaped quotes', function() {
            const buffer = Buffer.from('"a\\"b"', 'utf8');
            expect(findJsonValueEnd(buffer, 0)).to.be(buffer.length);
        });

        it('scans non-string values until the next delimiter', function() {
            const buffer = Buffer.from('12345,true', 'utf8');
            expect(findJsonValueEnd(buffer, 0)).to.be(5);
            expect(findJsonValueEnd(buffer, 6)).to.be(10);
        });

        it('returns -1 when the offset is beyond the buffer', function() {
            expect(findJsonValueEnd(Buffer.from('', 'utf8'), 0)).to.be(-1);
        });

    });

    describe('parseJsonValue', function() {

        it('parses string, numeric, boolean, and null scalars', function() {
            expect(parseJsonValue(Buffer.from('"Foo"', 'utf8'), 0, 5)).to.be('Foo');
            expect(parseJsonValue(Buffer.from('42', 'utf8'), 0, 2)).to.be(42);
            expect(parseJsonValue(Buffer.from('true', 'utf8'), 0, 4)).to.be(true);
            expect(parseJsonValue(Buffer.from('null', 'utf8'), 0, 4)).to.be(null);
        });

        it('returns undefined for empty or malformed scalar slices', function() {
            expect(parseJsonValue(Buffer.from('   ', 'utf8'), 0, 3)).to.be(undefined);
            expect(parseJsonValue(Buffer.from('10abc', 'utf8'), 0, 5)).to.be(undefined);
        });

    });

});




