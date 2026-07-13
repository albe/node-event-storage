import expect from 'expect.js';
import fs from 'fs-extra';
import JoinEventStream from '../src/JoinEventStream.js';
import EventStore from '../src/EventStore.js';

describe('JoinEventStream', function() {

    let stream, eventstore;
    const events = [{ type: 'foo' }, { type: 'bar' }, { type: 'baz' }];

    before(function (done) {
        fs.emptyDirSync('test/data');
        eventstore = new EventStore({
            storageDirectory: 'test/data'
        });
        eventstore.commit('foo', events[0], () => {
        eventstore.commit('bar', events[1], () => {
        eventstore.commit('foo', events[2], () => {
            done();
        });
        });
        });
    });

    after(function () {
        eventstore.close();
        eventstore = null;
    });

    it('makes the name available', function(){
        stream = new JoinEventStream('foo-bar', ['foo', 'bar'], eventstore);
        expect(stream.name).to.be('foo-bar');
    });

    it('throws if no name specified in constructor', function(){
        expect(() => new JoinEventStream()).to.throwError();
    });

    it('throws if no or invalid stream list specified in constructor', function(){
        expect(() => new JoinEventStream('foo-bar', 'foo', eventstore)).to.throwError();
        expect(() => new JoinEventStream('foo-bar', [], eventstore)).to.throwError();
    });

    it('throws if no EventStore specified in constructor', function(){
        expect(() => new JoinEventStream('foo-bar', ['foo', 'bar'])).to.throwError();
    });

    it('makes all events accessible as array', function(){
        stream = new JoinEventStream('foo-bar', ['foo', 'bar'], eventstore);
        expect(stream.events).to.eql(events);
    });

    it('returns all events consistently', function(){
        stream = new JoinEventStream('foo-bar', ['foo', 'bar'], eventstore);
        expect(stream.events).to.eql(stream.events);
    });

    it('can be iterated with for .. of', function(){
        stream = new JoinEventStream('foo-bar', ['foo', 'bar'], eventstore);
        let i = 0;
        for (let event of stream) {
            expect(event).to.eql(events[i++]);
        }
    });

    it('is a readable stream', function(done){
        stream = new JoinEventStream('foo-bar', ['foo', 'bar'], eventstore);
        let i = 0;
        stream.on('data', (event) => {
            expect(event).to.eql(events[i++]);
            if (i === events.length) {
                done();
            }
        });
    });

    it('can limit events fetched with min and max revision', function(){
        stream = new JoinEventStream('foo-bar', ['foo', 'bar'], eventstore, 1, 2);
        const fetchedEvents = stream.events;

        expect(fetchedEvents.length).to.be(2);
        expect(fetchedEvents[0]).to.eql(events[0]);
        expect(fetchedEvents[1]).to.eql(events[1]);
    });

    it('returns no events when a global revision window does not include any event from the joined stream', function() {
        stream = new JoinEventStream('foo-only', ['foo'], eventstore, 2, 2);
        expect(stream.events).to.eql([]);
    });

    it('can fetch events from the end only', function(){
        stream = new JoinEventStream('foo-bar', ['foo', 'bar'], eventstore, -2, -1);
        const fetchedEvents = stream.events;

        expect(fetchedEvents.length).to.be(2);
        expect(fetchedEvents[0]).to.eql(events[1]);
        expect(fetchedEvents[1]).to.eql(events[2]);
    });

    it('allows specifying version range in natural language', function(){
        stream = new JoinEventStream('foo-bar', ['foo', 'bar'], eventstore).fromStart().toEnd();

        let fetchedEvents = stream.events;
        expect(fetchedEvents.length).to.be(3);
        expect(fetchedEvents).to.eql(events);

        fetchedEvents = stream.reset().first(2).events;
        expect(fetchedEvents.length).to.be(2);
        expect(fetchedEvents[0]).to.eql(events[0]);
        expect(fetchedEvents[1]).to.eql(events[1]);

        fetchedEvents = stream.reset().last(2).events;
        expect(fetchedEvents.length).to.be(2);
        expect(fetchedEvents[0]).to.eql(events[1]);
        expect(fetchedEvents[1]).to.eql(events[2]);

        fetchedEvents = stream.reset().from(2).toEnd().events;
        expect(fetchedEvents.length).to.be(2);
        expect(fetchedEvents[0]).to.eql(events[1]);
        expect(fetchedEvents[1]).to.eql(events[2]);

        fetchedEvents = stream.reset().fromEnd().toStart().events;
        expect(fetchedEvents.length).to.be(3);
        expect(fetchedEvents).to.eql(Array.from(events).reverse());

        fetchedEvents = stream.reset().fromStart().toEnd().backwards().events;
        expect(fetchedEvents.length).to.be(3);
        expect(fetchedEvents).to.eql(Array.from(events).reverse());
    });

    it('is empty when stream does not exist', function(){
        stream = new JoinEventStream('foo-bar', ['baz'], eventstore);
        expect(stream.events).to.be.eql([]);
    });

    describe('forEach', function(){

        it('invokes a callback with payload, metadata and stream name', function(){
            stream = new JoinEventStream('foo-bar', ['foo', 'bar'], eventstore);
            let i = 0;
            stream.forEach((event, metadata, stream) => {
                expect(event).to.eql(events[i++]);
                expect(stream).to.be(i === 2 ? 'bar' : 'foo');
            });
        });

    });

    describe('raw mode', function() {

        it('yields newline-delimited JSON buffers when predicate is true', function() {
            const rawStream = new JoinEventStream('foo-bar', ['foo', 'bar'], eventstore, 1, -1, null, true);
            const chunks = [...rawStream];

            expect(chunks.length).to.be(3);
            chunks.forEach((chunk) => {
                expect(Buffer.isBuffer(chunk)).to.be(true);
                expect(chunk.at(-1)).to.be(0x0A);
                expect(() => JSON.parse(chunk.toString('utf8'))).to.not.throwError();
            });
        });

        it('supports object matchers in raw mode', function() {
            const rawStream = new JoinEventStream('foo-bar', ['foo', 'bar'], eventstore, 1, -1, {
                payload: { type: 'bar' }
            }, true);
            const chunks = [...rawStream];

            expect(chunks.length).to.be(1);
            const parsed = JSON.parse(chunks[0].toString('utf8'));
            expect(parsed.payload.type).to.be('bar');
        });

        it('passes raw document buffers to function matchers in raw mode', function() {
            let calledWithBuffer = false;
            const rawStream = new JoinEventStream('foo-bar', ['foo', 'bar'], eventstore, 1, -1, (buffer) => {
                calledWithBuffer = Buffer.isBuffer(buffer);
                return buffer.includes(Buffer.from('"baz"', 'utf8'));
            }, true);
            const chunks = [...rawStream];

            expect(calledWithBuffer).to.be(true);
            expect(chunks.length).to.be(1);
        });

    });

    // Multiple joined type-streams over the same write stream all read from that one partition. The
    // k-way merge holds the head of every stream at once, while raw reads return transient views into a
    // shared read/write buffer. Advancing one stream must not clobber another stream's pending head.
    describe('raw mode buffer aliasing across a shared partition', function() {

        const raceDir = 'test/data/raw-join-race';

        async function seedSharedPartition(config, flush) {
            fs.emptyDirSync(raceDir);
            const store = new EventStore('race', { storageDirectory: raceDir, typeAccessor: 'type', ...config });
            await new Promise(resolve => store.on('ready', resolve));

            const count = 40;
            const events = [];
            for (let i = 0; i < count; i++) {
                events.push({ type: i % 2 === 0 ? 'A' : 'B', seq: i, pad: 'x'.repeat(40) });
            }
            // All events go to a single write stream, so both type streams read from the same partition.
            if (flush) {
                await new Promise(resolve => store.commit('entity/1', events, resolve));
            } else {
                store.commit('entity/1', events);
            }
            return { store, count };
        }

        function assertCompleteJoin(store, count) {
            const rawStream = new JoinEventStream('A-B', ['A', 'B'], store, 1, -1, null, true);
            const seqs = [...rawStream].map(chunk => JSON.parse(chunk.toString('utf8')).payload.seq);
            expect(seqs.length).to.be(count);
            expect(seqs.slice().sort((a, b) => a - b)).to.eql(seqs.map((_, i) => i));
        }

        it('yields every flushed record exactly once when the read buffer refills mid-merge', async function() {
            // Tiny read buffer forces a refill on almost every read, invalidating stale head views.
            const { store, count } = await seedSharedPartition({ storageConfig: { readBufferSize: 256 } }, true);
            assertCompleteJoin(store, count);
            store.close();
        });

        it('yields valid, complete NDJSON when queried right after a write', async function() {
            // Small write buffer flushes partial batches to disk during the commit, so an immediate
            // read mixes still-buffered and just-flushed documents through the shared read buffer.
            const { store, count } = await seedSharedPartition({
                storageConfig: { readBufferSize: 256, writeBufferSize: 512 }
            }, false);
            assertCompleteJoin(store, count);
            store.close();
        });

    });

});
