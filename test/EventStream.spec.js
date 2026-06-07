import expect from 'expect.js';
import fs from 'fs-extra';
import EventStream from '../src/EventStream.js';
import EventStore from '../src/EventStore.js';

describe('EventStream', function() {

    let stream, mockEventStore;
    const events = ['foo', 'bar', 'baz'];

    beforeEach(function () {
        // This is pretty ugly testing internals, but that's only because we extracted this ugliness out of the EventStore
        mockEventStore = {
            streams: {
                'foo': {
                    index: {
                        name: 'foo-index',
                        length: events.length
                    }
                }
            },
            storage: {
                *readRange(from, until = -1) {
                    mockEventStore.storage.from = from;
                    mockEventStore.storage.until = until;
                    for (let event of events) {
                        yield { stream: 'foo', payload: event, metadata: { occuredAt: 12345 } };
                    }
                }
            }
        };
        stream = new EventStream('foo', mockEventStore);
    });

    it('makes the name available', function(){
        expect(stream.name).to.be('foo');
    });

    it('has version -1 if stream does not exist', function(){
        stream = new EventStream('foo-bar-baz', mockEventStore);
        expect(stream.version).to.be(-1);
    });

    it('makes the version available', function(){
        expect(stream.version).to.be(events.length);
    });

    it('adjusts the version to given maxRevision constraint', function(){
        stream = new EventStream('foo', mockEventStore, 0, -2);
        expect(stream.version).to.be(events.length - 1);
    });

    it('throws if no name specified in constructor', function(){
        expect(() => new EventStream()).to.throwError(/stream name/);
    });

    it('throws if empty name specified in constructor', function(){
        expect(() => new EventStream('')).to.throwError(/stream name/);
    });

    it('throws if no EventStore specified in constructor', function(){
        expect(() => new EventStream('foo')).to.throwError(/EventStore/);
    });

    it('makes all events accessible as array', function(){
        expect(stream.events).to.eql(events);
    });

    it('returns all events consistently', function(){
        expect(stream.events).to.eql(stream.events);
    });

    it('can be iterated with for .. of', function(){
        let i = 0;
        for (let event of stream) {
            expect(event).to.be(events[i++]);
        }
    });

    it('is a readable stream', function(){
        let i = 0;
        stream.on('data', (event) => {
            expect(event).to.be(events[i++]);
        });
    });

    it('accepts revisions as 1-based index', function(){
        stream = new EventStream('foo', mockEventStore, 1, 2);
        const events = stream.events;

        expect(mockEventStore.storage.from).to.be(1);
        expect(mockEventStore.storage.until).to.be(2);
    });

    it('adjusts negative revisions to stream length', function(){
        stream = new EventStream('foo', mockEventStore, -1, -1);
        // read all and convert to array
        const events = stream.events;

        expect(mockEventStore.storage.from).to.be(events.length);
        expect(mockEventStore.storage.until).to.be(events.length);
    });

    it('allows specifying version range in natural language', function(){
        stream.fromStart().toEnd();
        // read all and convert to array
        let events = stream.events;
        expect(mockEventStore.storage.from).to.be(1);
        expect(mockEventStore.storage.until).to.be(events.length);

        events = stream.reset().first(2).events;
        expect(mockEventStore.storage.from).to.be(1);
        expect(mockEventStore.storage.until).to.be(2);

        events = stream.reset().last(2).events;
        expect(mockEventStore.storage.from).to.be(events.length - 1);
        expect(mockEventStore.storage.until).to.be(events.length);

        events = stream.reset().from(2).toEnd().events;
        expect(mockEventStore.storage.from).to.be(2);
        expect(mockEventStore.storage.until).to.be(events.length);

        events = stream.reset().fromEnd().toStart().events;
        expect(mockEventStore.storage.from).to.be(events.length);
        expect(mockEventStore.storage.until).to.be(1);

        events = stream.reset().fromStart().toEnd().backwards().events;
        expect(mockEventStore.storage.from).to.be(events.length);
        expect(mockEventStore.storage.until).to.be(1);
    });

    it('is empty when stream does not exist', function(){
        stream = new EventStream('bar', mockEventStore);
        expect(stream.events).to.be.eql([]);
    });

    describe('forEach', function(){

        it('invokes a callback with payload, metadata and stream name', function(){
            let i = 0;
            stream.forEach((event, metadata, stream) => {
                expect(event).to.be(events[i++]);
                expect(metadata).to.eql({ occuredAt: 12345 });
                expect(stream).to.be('foo');
            });
        });

    });

    describe('raw mode', function() {

        let rawStore;

        before(function(done) {
            fs.emptyDirSync('test/data/eventstream-raw');
            rawStore = new EventStore({
                storageDirectory: 'test/data/eventstream-raw'
            });

            rawStore.commit('foo', { type: 'created', value: 1 }, () => {
                rawStore.commit('foo', { type: 'updated', value: 2 }, () => done());
            });
        });

        after(function() {
            rawStore.close();
            rawStore = null;
        });

        it('yields newline-delimited JSON buffers when predicate is true', function() {
            const rawStream = new EventStream('foo', rawStore, 1, -1, null, true);
            const chunks = [...rawStream];

            expect(chunks.length).to.be(2);
            chunks.forEach((chunk) => {
                expect(Buffer.isBuffer(chunk)).to.be(true);
                expect(chunk.at(-1)).to.be(0x0A);
                expect(() => JSON.parse(chunk.toString('utf8'))).to.not.throwError();
            });
        });

        it('supports object matchers in raw mode', function() {
            const rawStream = new EventStream('foo', rawStore, 1, -1, { payload: { type: 'updated' } }, true);
            const chunks = [...rawStream];

            expect(chunks.length).to.be(1);
            const parsed = JSON.parse(chunks[0].toString('utf8'));
            expect(parsed.payload.type).to.be('updated');
        });

        it('passes raw document buffers to function matchers in raw mode', function() {
            let calledWithBuffer = false;
            const rawStream = new EventStream('foo', rawStore, 1, -1, (buffer) => {
                calledWithBuffer = Buffer.isBuffer(buffer);
                return buffer.includes(Buffer.from('"updated"', 'utf8'));
            }, true);
            const chunks = [...rawStream];

            expect(calledWithBuffer).to.be(true);
            expect(chunks.length).to.be(1);
        });

        it('builds the raw object matcher lazily on first consumption', function() {
            const rawStream = new EventStream('foo', rawStore, 1, -1, { payload: { type: 'created' } }, true);
            expect(rawStream.rawMatcher).to.be(null);

            rawStream.next();

            expect(typeof rawStream.rawMatcher).to.be('function');
        });

        it('activates raw mode when predicate argument is true (shorthand)', function() {
            const rawStream = new EventStream('foo', rawStore, 1, -1, true);
            expect(rawStream.raw).to.be(true);
            expect(rawStream.predicate).to.be(null);
            const chunks = [...rawStream];
            expect(chunks.length).to.be(2);
            expect(Buffer.isBuffer(chunks[0])).to.be(true);
        });

        it('emits Buffer chunks when used as a Readable stream in raw mode', function(done) {
            const rawStream = new EventStream('foo', rawStore, 1, -1, null, true);
            const chunks = [];
            rawStream.on('data', (chunk) => chunks.push(chunk));
            rawStream.once('end', () => {
                expect(chunks.length).to.be(2);
                expect(Buffer.isBuffer(chunks[0])).to.be(true);
                expect(chunks[0].at(-1)).to.be(0x0A);
                done();
            });
        });

    });

    describe('filter()', function() {

        it('sets a predicate and resets the iterator', function() {
            const result = stream.filter(payload => payload === 'foo');
            expect(result).to.be(stream);
            expect(typeof stream.predicate).to.be('function');
            expect(stream.events).to.eql(['foo']);
        });

        it('clears the predicate when called with no argument', function() {
            stream.filter(payload => payload === 'foo');
            stream.filter(null);
            expect(stream.predicate).to.be(null);
            expect(stream.events).to.eql(events);
        });

        it('delegates to Readable.filter() when called with options', async function() {
            const filteredReadable = stream.filter((payload) => payload === 'foo', { concurrency: 1 });
            expect(filteredReadable).not.to.be(stream);

            const values = [];
            for await (const value of filteredReadable) {
                values.push(value);
            }
            expect(values).to.eql(['foo']);
        });

    });

    describe('where()', function() {

        it('applies matcher semantics and resets the iterator', function() {
            const filtered = stream.where({ payload: 'foo' });
            expect(filtered).to.be(stream);
            expect(stream.events).to.eql(['foo']);
        });

    });

    describe('object matcher (non-raw mode)', function() {

        it('filters events by an object matcher against stream/payload/metadata', function() {
            const filtered = stream.filter({ payload: 'foo' });
            expect(filtered.events).to.eql(['foo']);
        });

    });

});
