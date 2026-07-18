import expect from 'expect.js';
import {
    matches,
    buildRawBufferMatcher,
    buildMetadataForMatcher,
    buildMatcherFromMetadata,
    buildMatcherFn,
    buildMetadataHeader,
    createHmac
} from '../src/utils/metadataUtil.js';

describe('metadataUtil', function () {

    describe('buildRawBufferMatcher', function () {

        it('matches top-level scalar properties in raw JSON buffers', function () {
            const matchesTypeFoo = buildRawBufferMatcher({type: 'Foo'});
            const buffer = Buffer.from('{"type":"Foo","id":1}', 'utf8');
            expect(matchesTypeFoo(buffer)).to.be(true);
        });

        it('does not match nested values with the same key/value', function () {
            const matchesTypeFoo = buildRawBufferMatcher({type: 'Foo'});
            const buffer = Buffer.from('{"payload":{"type":"Foo"},"type":"Bar"}', 'utf8');
            expect(matchesTypeFoo(buffer)).to.be(false);
        });

        it('supports matchers with multiple top-level properties', function () {
            const matcher = buildRawBufferMatcher({type: 'Foo', version: 3});
            const buffer = Buffer.from('{"type":"Foo","version":3,"payload":"ok"}', 'utf8');
            expect(matcher(buffer)).to.be(true);
        });

        it('requires compact key-value separators without whitespace', function () {
            const matcher = buildRawBufferMatcher({type: 'Foo', enabled: true});
            const buffer = Buffer.from('{ "type" : "Foo", "enabled" : true }', 'utf8');
            expect(matcher(buffer)).to.be(false);
        });

        it('supports nested matcher objects via the scoped fallback', function () {
            const matcher = buildRawBufferMatcher({payload: {type: 'Foo'}});
            const buffer = Buffer.from('{"payload":{"type":"Foo"},"id":1}', 'utf8');
            expect(matcher(buffer)).to.be(true);
        });

    });

    describe('buildRawBufferMatcher (nested/object-array mode)', function () {

        it('matches one-level nested object properties', function () {
            const matcher = buildRawBufferMatcher({payload: {type: 'Foo'}});
            const buffer = Buffer.from('{"payload":{"type":"Foo"},"id":1}', 'utf8');
            expect(matcher(buffer)).to.be(true);
        });

        it('does not match when nested value differs', function () {
            const matcher = buildRawBufferMatcher({payload: {type: 'Foo'}});
            const buffer = Buffer.from('{"payload":{"type":"Bar"},"id":1}', 'utf8');
            expect(matcher(buffer)).to.be(false);
        });

        it('matches one of two allowed nested scalar values', function () {
            const matcher = buildRawBufferMatcher({payload: {type: ['Foo', 'Bar']}});
            const fooBuffer = Buffer.from('{"payload":{"type":"Foo"},"id":1}', 'utf8');
            const barBuffer = Buffer.from('{"payload":{"type":"Bar"},"id":2}', 'utf8');
            const bazBuffer = Buffer.from('{"payload":{"type":"Baz"},"id":3}', 'utf8');

            expect(matcher(fooBuffer)).to.be(true);
            expect(matcher(barBuffer)).to.be(true);
            expect(matcher(bazBuffer)).to.be(false);
        });

        it('matches with array containing single scalar value', function () {
            const matcher = buildRawBufferMatcher({status: ['active']});
            const buffer = Buffer.from('{"status":"active"}', 'utf8');
            expect(matcher(buffer)).to.be(true);
        });

        it('matches nested array with single value', function () {
            const matcher = buildRawBufferMatcher({payload: {type: ['Foo']}});
            const buffer = Buffer.from('{"payload":{"type":"Foo"}}', 'utf8');
            expect(matcher(buffer)).to.be(true);
        });

        it('throws when matcher is not an object', function () {
            expect(() => buildRawBufferMatcher(null)).to.throwError(TypeError);
            expect(() => buildRawBufferMatcher(['Foo'])).to.throwError(TypeError);
            expect(() => buildRawBufferMatcher('Foo')).to.throwError(TypeError);
        });

        it('throws when an array matcher contains object values', function () {
            expect(() => buildRawBufferMatcher({type: [{value: 'Foo'}]})).to.throwError(TypeError);
        });

        it('matches every object when matcher is empty', function () {
            const matcher = buildRawBufferMatcher({});
            expect(matcher(Buffer.from('{"type":"Foo"}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"nested":{"value":1}}', 'utf8'))).to.be(true);
        });

        it('does not match non-object JSON buffers', function () {
            const matcher = buildRawBufferMatcher({type: 'Foo'});
            expect(matcher(Buffer.from('[{"type":"Foo"}]', 'utf8'))).to.be(false);
        });

        it('does not match when the required nested object key is missing', function () {
            const matcher = buildRawBufferMatcher({payload: {type: 'Foo'}});
            const buffer = Buffer.from('{"meta":{"type":"Foo"},"id":1}', 'utf8');
            expect(matcher(buffer)).to.be(false);
        });

        it('matches string values containing escaped quotes', function () {
            const matcher = buildRawBufferMatcher({payload: {text: 'a"b'}});
            const matchingBuffer = Buffer.from('{"payload":{"text":"a\\\"b"}}', 'utf8');
            const nonMatchingBuffer = Buffer.from('{"payload":{"text":"ab"}}', 'utf8');

            expect(matcher(matchingBuffer)).to.be(true);
            expect(matcher(nonMatchingBuffer)).to.be(false);
        });

        it('matches when escaped characters appear before the matched key on the same level', function () {
            const matcher = buildRawBufferMatcher({type: 'Foo'});
            const buffer = Buffer.from('{"note":"a\\\\b and \\\"quoted\\\"","type":"Foo"}', 'utf8');
            expect(matcher(buffer)).to.be(true);
        });

        it('does not match malformed objects with a premature closing brace before the key', function () {
            const matcher = buildRawBufferMatcher({type: 'Foo'});
            const buffer = Buffer.from('{"payload":{"id":1}},"type":"Foo"}', 'utf8');
            expect(matcher(buffer)).to.be(false);
        });

        it('does not match nested object keys that are only present deeper than the requested level', function () {
            const matcher = buildRawBufferMatcher({payload: {source: {kind: 'A'}}});
            const buffer = Buffer.from('{"payload":{"wrapper":{"source":{"kind":"A"}}}}', 'utf8');
            expect(matcher(buffer)).to.be(false);
        });

        it('does not match when a nested object exists but its nested value differs', function () {
            const matcher = buildRawBufferMatcher({payload: {source: {kind: 'A'}}});
            const buffer = Buffer.from('{"payload":{"source":{"kind":"B"}}}', 'utf8');
            expect(matcher(buffer)).to.be(false);
        });

        it('does not match scalar prefixes like 30 for an expected value of 3', function () {
            const matcher = buildRawBufferMatcher({version: 3});
            const buffer = Buffer.from('{"version":30}', 'utf8');
            expect(matcher(buffer)).to.be(false);
        });

    });

    describe('operators: $gt, $gte, $lt, $lte', function () {

        it('matches values greater than threshold with $gt', function () {
            const matcher = buildRawBufferMatcher({amount: {$gt: 100}});
            const buffer = Buffer.from('{"amount":150}', 'utf8');
            expect(matcher(buffer)).to.be(true);
        });

        it('does not match values equal or less than threshold with $gt', function () {
            const matcher = buildRawBufferMatcher({amount: {$gt: 100}});
            expect(matcher(Buffer.from('{"amount":100}', 'utf8'))).to.be(false);
            expect(matcher(Buffer.from('{"amount":50}', 'utf8'))).to.be(false);
        });

        it('matches values greater or equal to threshold with $gte', function () {
            const matcher = buildRawBufferMatcher({amount: {$gte: 100}});
            expect(matcher(Buffer.from('{"amount":150}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"amount":100}', 'utf8'))).to.be(true);
        });

        it('does not match values less than threshold with $gte', function () {
            const matcher = buildRawBufferMatcher({amount: {$gte: 100}});
            expect(matcher(Buffer.from('{"amount":99}', 'utf8'))).to.be(false);
        });

        it('matches string values with $gte', function () {
            const matcher = buildRawBufferMatcher({status: {$gte: 'pending'}});
            expect(matcher(Buffer.from('{"status":"pending"}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"status":"submitted"}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"status":"draft"}', 'utf8'))).to.be(false);
        });

        it('combines multiple operators in one matcher', function () {
            const matcher = buildRawBufferMatcher({version: {$gte: 2, $lt: 5}});
            expect(matcher(Buffer.from('{"version":2}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"version":3}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"version":4}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"version":5}', 'utf8'))).to.be(false);
            expect(matcher(Buffer.from('{"version":1}', 'utf8'))).to.be(false);
        });

        it('combines multiple numeric operators including $ne', function () {
            const matcher = buildRawBufferMatcher({version: {$gt: 1, $lt: 5, $ne: 3}});
            expect(matcher(Buffer.from('{"version":2}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"version":3}', 'utf8'))).to.be(false);
            expect(matcher(Buffer.from('{"version":4}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"version":5}', 'utf8'))).to.be(false);
        });

        it('supports multiple string operators via generic fallback', function () {
            const matcher = buildRawBufferMatcher({status: {$gte: 'b', $lt: 'd'}});
            expect(matcher(Buffer.from('{"status":"b"}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"status":"c"}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"status":"d"}', 'utf8'))).to.be(false);
            expect(matcher(Buffer.from('{"status":"a"}', 'utf8'))).to.be(false);
        });

        it('matches ISO datetime strings in a nested range', function () {
            const matcher = buildRawBufferMatcher({
                payload: {
                    at: {
                        $gte: '2021-06-09T21:03:28.297Z',
                        $lt: '2021-06-09T21:41:53.775Z'
                    }
                }
            });

            const events = [
                {payload: {at: '2021-06-09T21:03:28.297Z'}},
                {payload: {at: '2021-06-09T21:20:00.000Z'}},
                {payload: {at: '2021-06-09T21:41:53.774Z'}},
                {payload: {at: '2021-06-09T21:03:28.296Z'}},
                {payload: {at: '2021-06-09T21:41:53.775Z'}}
            ];

            const matching = events.filter(event => matcher(Buffer.from(JSON.stringify(event), 'utf8')));
            expect(matching).to.eql([
                {payload: {at: '2021-06-09T21:03:28.297Z'}},
                {payload: {at: '2021-06-09T21:20:00.000Z'}},
                {payload: {at: '2021-06-09T21:41:53.774Z'}}
            ]);
        });

        it('works with nested objects', function () {
            const matcher = buildRawBufferMatcher({payload: {amount: {$gte: 50}}});
            expect(matcher(Buffer.from('{"payload":{"amount":50}}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"payload":{"amount":100}}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"payload":{"amount":25}}', 'utf8'))).to.be(false);
        });

        it('works with floating point numbers', function () {
            const matcher = buildRawBufferMatcher({price: {$gt: 19.99}});
            expect(matcher(Buffer.from('{"price":20.0}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"price":19.99}', 'utf8'))).to.be(false);
            expect(matcher(Buffer.from('{"price":19.98}', 'utf8'))).to.be(false);
        });

        it('works with negative numbers', function () {
            const matcher = buildRawBufferMatcher({temperature: {$gte: -10}});
            expect(matcher(Buffer.from('{"temperature":-5}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"temperature":-10}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"temperature":-15}', 'utf8'))).to.be(false);
        });

        it('matches values less than threshold with $lt', function () {
            const matcher = buildRawBufferMatcher({amount: {$lt: 100}});
            expect(matcher(Buffer.from('{"amount":50}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"amount":100}', 'utf8'))).to.be(false);
            expect(matcher(Buffer.from('{"amount":150}', 'utf8'))).to.be(false);
        });

        it('matches values less or equal to threshold with $lte', function () {
            const matcher = buildRawBufferMatcher({amount: {$lte: 100}});
            expect(matcher(Buffer.from('{"amount":50}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"amount":100}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"amount":101}', 'utf8'))).to.be(false);
        });

        it('matches with standalone $eq operator', function () {
            const matcher = buildRawBufferMatcher({status: {$eq: 'active'}});
            expect(matcher(Buffer.from('{"status":"active"}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"status":"inactive"}', 'utf8'))).to.be(false);
        });

        it('matches with $ne operator', function () {
            const matcher = buildRawBufferMatcher({status: {$ne: 'active'}});
            expect(matcher(Buffer.from('{"status":"inactive"}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"status":"active"}', 'utf8'))).to.be(false);
        });

        it('lone $ne does not match when the key is absent', function () {
            const matcher = buildRawBufferMatcher({status: {$ne: 'active'}});
            expect(matcher(Buffer.from('{"other":"active"}', 'utf8'))).to.be(false);
        });

        it('lone $ne ignores matching value at the wrong nesting level', function () {
            // "status":"active" appears only inside payload — must not match the top-level $ne check
            const matcher = buildRawBufferMatcher({status: {$ne: 'active'}});
            expect(matcher(Buffer.from('{"payload":{"status":"active"},"status":"inactive"}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"payload":{"status":"inactive"},"status":"active"}', 'utf8'))).to.be(false);
        });

        it('lone $ne matches a numeric value that differs', function () {
            const matcher = buildRawBufferMatcher({version: {$ne: 3}});
            expect(matcher(Buffer.from('{"version":4}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"version":3}', 'utf8'))).to.be(false);
        });

        it('handles fractional numbers with operators', function () {
            const matcher = buildRawBufferMatcher({price: {$lte: 19.99}});
            expect(matcher(Buffer.from('{"price":19.99}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"price":19.98}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"price":20.0}', 'utf8'))).to.be(false);
        });

        it('matches with $eq and $ne on string values', function () {
            const eqMatcher = buildRawBufferMatcher({type: {$eq: 'EventType'}});
            expect(eqMatcher(Buffer.from('{"type":"EventType"}', 'utf8'))).to.be(true);
            expect(eqMatcher(Buffer.from('{"type":"OtherType"}', 'utf8'))).to.be(false);
        });

        it('matches when multiple operators and properties are combined', function () {
            const matcher = buildRawBufferMatcher({
                status: {$ne: 'draft'},
                priority: {$gte: 5}
            });
            expect(matcher(Buffer.from('{"status":"published","priority":7}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"status":"draft","priority":7}', 'utf8'))).to.be(false);
            expect(matcher(Buffer.from('{"status":"published","priority":3}', 'utf8'))).to.be(false);
        });

        it('uses generic operator checks when operator buffer matcher is disabled', function () {
            const matcher = buildRawBufferMatcher(
                {amount: {$gt: 100}},
                {enableOperatorBufferMatcher: false}
            );
            expect(matcher(Buffer.from('{"amount":150}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"amount":100}', 'utf8'))).to.be(false);
        });

        it('rejects early via fast scalar pattern when string equals precedes operator', function () {
            // status:'active' is a fast scalar pattern; amount:{$gte:50} is slow.
            // With reordering, the status check runs first in both preCheck and matchesNode,
            // so documents with wrong status never reach the numeric operator.
            const matcher = buildRawBufferMatcher({status: 'active', amount: {$gte: 50}});
            expect(matcher(Buffer.from('{"status":"active","amount":100}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"status":"inactive","amount":100}', 'utf8'))).to.be(false);
            expect(matcher(Buffer.from('{"status":"active","amount":10}', 'utf8'))).to.be(false);
        });

        it('rejects early when type equality is listed after a multi-operator in source order', function () {
            // Matcher keys: amount first (slow), type second (fast).
            // After reordering, type:"Foo" moves to front and acts as a prefilter.
            const matcher = buildRawBufferMatcher({amount: {$gt: 10, $lt: 100}, type: 'Foo'});
            expect(matcher(Buffer.from('{"amount":50,"type":"Foo"}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"amount":50,"type":"Bar"}', 'utf8'))).to.be(false);
            expect(matcher(Buffer.from('{"amount":150,"type":"Foo"}', 'utf8'))).to.be(false);
        });

        it('matches with lone $has for tag arrays', function () {
            const matcher = buildRawBufferMatcher({tags: {$has: 'featured'}});
            expect(matcher(Buffer.from('{"tags":["featured","new"]}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"tags":["new","featured"]}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"tags":["archived"]}', 'utf8'))).to.be(false);
            expect(matcher(Buffer.from('{"tags":[]}', 'utf8'))).to.be(false);
            expect(matcher(Buffer.from('{"other":["featured"]}', 'utf8'))).to.be(false);
        });

        it('$has ignores matches inside nested objects/arrays at wrong depth', function () {
            const matcher = buildRawBufferMatcher({tags: {$has: 'x'}});
            // "x" appears only inside a nested object, not as an element of tags itself
            expect(matcher(Buffer.from('{"tags":[{"inner":"x"}]}', 'utf8'))).to.be(false);
            // "x" appears inside a nested array element, not as an element of tags itself
            expect(matcher(Buffer.from('{"tags":[["x"]]}', 'utf8'))).to.be(false);
            // "x" appears as an actual element
            expect(matcher(Buffer.from('{"tags":[{"inner":"y"},"x"]}', 'utf8'))).to.be(true);
        });

        it('$has combines with other property matchers', function () {
            const matcher = buildRawBufferMatcher({type: 'OrderPlaced', tags: {$has: 'vip'}});
            expect(matcher(Buffer.from('{"type":"OrderPlaced","tags":["vip"]}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"type":"OrderPlaced","tags":["regular"]}', 'utf8'))).to.be(false);
            expect(matcher(Buffer.from('{"type":"OrderCancelled","tags":["vip"]}', 'utf8'))).to.be(false);
        });

        it('matches with $hasAny for tag arrays', function () {
            const matcher = buildRawBufferMatcher({tags: {$hasAny: ['featured', 'new']}});
            expect(matcher(Buffer.from('{"tags":["featured","archived"]}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"tags":["new","archived"]}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"tags":["featured","new"]}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"tags":["archived"]}', 'utf8'))).to.be(false);
            expect(matcher(Buffer.from('{"tags":[]}', 'utf8'))).to.be(false);
            expect(matcher(Buffer.from('{"other":["featured"]}', 'utf8'))).to.be(false);
        });

        it('$hasAny ignores matches inside nested objects/arrays at wrong depth', function () {
            const matcher = buildRawBufferMatcher({tags: {$hasAny: ['x', 'y']}});
            expect(matcher(Buffer.from('{"tags":[{"inner":"x"}]}', 'utf8'))).to.be(false);
            expect(matcher(Buffer.from('{"tags":[["x"]]}', 'utf8'))).to.be(false);
            expect(matcher(Buffer.from('{"tags":[{"inner":"y"},"x"]}', 'utf8'))).to.be(true);
        });

        it('$hasAny combines with other property matchers', function () {
            const matcher = buildRawBufferMatcher({type: 'OrderPlaced', tags: {$hasAny: ['vip', 'premium']}});
            expect(matcher(Buffer.from('{"type":"OrderPlaced","tags":["vip"]}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"type":"OrderPlaced","tags":["premium"]}', 'utf8'))).to.be(true);
            expect(matcher(Buffer.from('{"type":"OrderPlaced","tags":["regular"]}', 'utf8'))).to.be(false);
            expect(matcher(Buffer.from('{"type":"OrderCancelled","tags":["vip"]}', 'utf8'))).to.be(false);
        });

    });

    describe('matches with operators ($gt, $gte, $lt, $lte, $eq, $ne)', function () {

        it('matches with $gt', function () {
            expect(matches({amount: 150}, {amount: {$gt: 100}})).to.be(true);
            expect(matches({amount: 100}, {amount: {$gt: 100}})).to.be(false);
            expect(matches({amount: 50}, {amount: {$gt: 100}})).to.be(false);
        });

        it('matches with $gte', function () {
            expect(matches({amount: 150}, {amount: {$gte: 100}})).to.be(true);
            expect(matches({amount: 100}, {amount: {$gte: 100}})).to.be(true);
            expect(matches({amount: 99}, {amount: {$gte: 100}})).to.be(false);
        });

        it('matches with $lt', function () {
            expect(matches({amount: 50}, {amount: {$lt: 100}})).to.be(true);
            expect(matches({amount: 100}, {amount: {$lt: 100}})).to.be(false);
            expect(matches({amount: 150}, {amount: {$lt: 100}})).to.be(false);
        });

        it('matches with $lte', function () {
            expect(matches({amount: 50}, {amount: {$lte: 100}})).to.be(true);
            expect(matches({amount: 100}, {amount: {$lte: 100}})).to.be(true);
            expect(matches({amount: 101}, {amount: {$lte: 100}})).to.be(false);
        });

        it('matches with $eq', function () {
            expect(matches({status: 'active'}, {status: {$eq: 'active'}})).to.be(true);
            expect(matches({status: 'inactive'}, {status: {$eq: 'active'}})).to.be(false);
        });

        it('matches with $ne', function () {
            expect(matches({status: 'inactive'}, {status: {$ne: 'active'}})).to.be(true);
            expect(matches({status: 'active'}, {status: {$ne: 'active'}})).to.be(false);
        });

        it('combines multiple operators', function () {
            expect(matches({version: 3}, {version: {$gte: 2, $lt: 5}})).to.be(true);
            expect(matches({version: 1}, {version: {$gte: 2, $lt: 5}})).to.be(false);
            expect(matches({version: 5}, {version: {$gte: 2, $lt: 5}})).to.be(false);
        });

        it('throws on unknown operator', function () {
            expect(() => matches({x: 1}, {x: {$unknown: 1}})).to.throwError(TypeError);
        });

        it('does not confuse operator objects with plain nested matchers', function () {
            // Plain nested object (no $ keys) → deep match
            expect(matches({meta: {kind: 'A'}}, {meta: {kind: 'A'}})).to.be(true);
            // Operator object → range comparison
            expect(matches({amount: 50}, {amount: {$gt: 10}})).to.be(true);
        });

        it('reuses compiled operator checks on repeated matcher objects', function () {
            const matcher = {amount: {$gte: 10}};
            expect(matches({amount: 12}, matcher)).to.be(true);
            expect(matches({amount: 9}, matcher)).to.be(false);
            expect(matches({amount: 11}, matcher)).to.be(true);
        });

        it('matches with $lt', function () {
            expect(matches({amount: 50}, {amount: {$lt: 100}})).to.be(true);
            expect(matches({amount: 100}, {amount: {$lt: 100}})).to.be(false);
            expect(matches({amount: 150}, {amount: {$lt: 100}})).to.be(false);
        });

        it('matches with $lte', function () {
            expect(matches({amount: 50}, {amount: {$lte: 100}})).to.be(true);
            expect(matches({amount: 100}, {amount: {$lte: 100}})).to.be(true);
            expect(matches({amount: 101}, {amount: {$lte: 100}})).to.be(false);
        });

        it('matches with $ne', function () {
            expect(matches({status: 'inactive'}, {status: {$ne: 'active'}})).to.be(true);
            expect(matches({status: 'active'}, {status: {$ne: 'active'}})).to.be(false);
        });

        it('matches when operator value is undefined (matches all non-undefined values)', function () {
            expect(matches({amount: 50}, {amount: undefined})).to.be(true);
            expect(matches({amount: undefined}, {amount: undefined})).to.be(true);
        });

        it('matches when document property is undefined', function () {
            expect(matches({}, {missing: undefined})).to.be(true);
            expect(matches({other: 1}, {missing: {$gt: 10}})).to.be(false);
        });

        it('handles array equality matching', function () {
            expect(matches({status: 'active'}, {status: ['active', 'pending']})).to.be(true);
            expect(matches({status: 'completed'}, {status: ['active', 'pending']})).to.be(false);
        });

        it('handles array document value with $has operator (containment check)', function () {
            expect(matches({tags: ['a', 'b']}, {tags: {$has: 'a'}})).to.be(true);
            expect(matches({tags: ['a', 'b']}, {tags: {$has: 'b'}})).to.be(true);
            expect(matches({tags: ['a', 'b']}, {tags: {$has: 'c'}})).to.be(false);
            expect(matches({tags: []}, {tags: {$has: 'a'}})).to.be(false);
            expect(matches({}, {tags: {$has: 'a'}})).to.be(false);
        });

        it('$has returns false when the document value is not an array', function () {
            expect(matches({tags: 'a'}, {tags: {$has: 'a'}})).to.be(false);
            expect(matches({tags: {a: 1}}, {tags: {$has: 'a'}})).to.be(false);
        });

        it('handles array document value with $hasAny operator (any-of containment)', function () {
            expect(matches({tags: ['a', 'b']}, {tags: {$hasAny: ['a', 'c']}})).to.be(true);
            expect(matches({tags: ['a', 'b']}, {tags: {$hasAny: ['b', 'c']}})).to.be(true);
            expect(matches({tags: ['a', 'b']}, {tags: {$hasAny: ['c', 'd']}})).to.be(false);
            expect(matches({tags: []}, {tags: {$hasAny: ['a']}})).to.be(false);
            expect(matches({}, {tags: {$hasAny: ['a']}})).to.be(false);
        });

        it('$hasAny returns false when the document value is not an array', function () {
            expect(matches({tags: 'a'}, {tags: {$hasAny: ['a']}})).to.be(false);
            expect(matches({tags: {a: 1}}, {tags: {$hasAny: ['a']}})).to.be(false);
        });

        it('scalar matcher against array document value no longer performs containment', function () {
            // Historical auto-containment removed in favor of the explicit $has operator.
            expect(matches({tags: ['a', 'b']}, {tags: 'a'})).to.be(false);
        });

        it('handles nested object matching without operators', function () {
            expect(matches({meta: {kind: 'A', version: 1}}, {meta: {kind: 'A'}})).to.be(true);
            expect(matches({meta: {kind: 'B'}}, {meta: {kind: 'A'}})).to.be(false);
        });

    });

    describe('matcher metadata helpers', function () {

        it('returns undefined when no matcher is provided', function () {
            expect(buildMetadataForMatcher(undefined, createHmac('secret'))).to.be(undefined);
        });

        it('serializes and restores function matchers with hmac', function () {
            const hmac = createHmac('secret');
            const matcher = event => event.type === 'Foo';
            const metadata = buildMetadataForMatcher(matcher, hmac);

            expect(metadata.matcher).to.be.a('string');
            expect(metadata.hmac).to.be(hmac(metadata.matcher));

            const restored = buildMatcherFromMetadata(metadata, hmac);
            expect(restored({type: 'Foo'})).to.be(true);
            expect(restored({type: 'Bar'})).to.be(false);
        });

        it('passes through object matchers unchanged in metadata', function () {
            const matcher = {payload: {type: 'Foo'}};
            const metadata = buildMetadataForMatcher(matcher, createHmac('secret'));
            const restored = buildMatcherFromMetadata(metadata, createHmac('secret'));

            expect(metadata).to.eql({matcher});
            expect(restored).to.eql(matcher);
        });

        it('builds type matcher functions for single-level paths', function () {
            const typeMatcher = buildMatcherFn('type');
            expect(typeMatcher('OrderPlaced')).to.eql({payload: {type: 'OrderPlaced'}});
        });

        it('builds type matcher functions for nested paths', function () {
            const typeMatcher = buildMatcherFn('meta.kind');
            expect(typeMatcher('OrderPlaced')).to.eql({payload: {meta: {kind: 'OrderPlaced'}}});
        });

        it('builds type matcher functions for deeply nested paths', function () {
            const typeMatcher = buildMatcherFn('deeply.nested.type');
            expect(typeMatcher('MyEvent')).to.eql({
                payload: {deeply: {nested: {type: 'MyEvent'}}}
            });
        });

        it('folds a $eq operator into a scalar matcher', function () {
            const scalar = buildMatcherFn('type', '$eq');
            expect(scalar('OrderPlaced')).to.eql({payload: {type: 'OrderPlaced'}});
        });

        it('wraps the leaf in the given operator for non-scalar operators', function () {
            const tagMatcher = buildMatcherFn('tags', '$has');
            expect(tagMatcher('featured')).to.eql({payload: {tags: {$has: 'featured'}}});
        });

        it('builds a padded metadata header', function () {
            const header = buildMetadataHeader('MAGICHDR', {version: 1});

            expect(header.slice(0, 8).toString('utf8')).to.be('MAGICHDR');
            expect(header.length % 16).to.be(0);
            expect(header.readUInt32BE(8)).to.be.greaterThan(0);
        });

        it('builds metadata header with large metadata object', function () {
            const largeMetadata = {key: 'value'.repeat(50)};
            const header = buildMetadataHeader('MAGICHDR', largeMetadata);

            expect(header.slice(0, 8).toString('utf8')).to.be('MAGICHDR');
            expect(header.length % 16).to.be(0);
        });

        it('creates different HMACs for different secrets', function () {
            const hmac1 = createHmac('secret1');
            const hmac2 = createHmac('secret2');
            const testString = 'test';

            expect(hmac1(testString)).not.to.be(hmac2(testString));
        });

    });

});

