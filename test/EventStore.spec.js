const expect = require('expect.js');
const fs = require('fs-extra');
const path = require('path');
const EventStore = require('../src/EventStore');

const storageDirectory = __dirname + '/data';

describe('EventStore', function() {

    let eventstore;

    beforeEach(function () {
        fs.emptyDirSync(storageDirectory);
    });

    afterEach(function () {
        if (eventstore) {
            eventstore.close();
        }
        eventstore = null;
    });

    it('basically works', function(done) {
        eventstore = new EventStore({
            storageDirectory
        });

        let events = [{foo: 'bar'}, {foo: 'baz'}, {foo: 'quux'}];
        eventstore.on('ready', () => {
            eventstore.commit('foo-bar', events, () => {
                const stream = eventstore.getEventStream('foo-bar');
                let i = 0;
                for (let event of stream) {
                    expect(event).to.eql(events[i++]);
                }
                done();
            });
        });
    });

    it('can be created with custom name', function(done) {
        eventstore = new EventStore('custom-store', {
            storageDirectory
        });

        eventstore.commit('foo-bar', [{ type: 'foo'}], () => {
            expect(fs.existsSync(path.join(storageDirectory, 'custom-store.foo-bar'))).to.be(true);
            done();
        });
    });

    it('throws when scanning of stream directory fails', function() {
        const fs = require('fs');
        const originalReaddir = fs.readdir;
        fs.readdir = (dir, callback) => callback(new Error('Something went wrong!'), null);

        expect(() => new EventStore({
            storageDirectory
        })).to.throwError(/Something went wrong!/);
        fs.readdir = originalReaddir;
    });

    it('repairs torn writes', function(done) {
        eventstore = new EventStore({
            storageDirectory
        });

        const events = [{foo: 'bar'.repeat(500)}];
        eventstore.on('ready', () => {
            eventstore.commit('foo-bar', events, () => {
                // Simulate a torn write (but indexes are still written)
                fs.truncateSync(eventstore.storage.getPartition('foo-bar').fileName, 512);

                // The previous instance was not closed, so the lock still exists
                const eventstore2 = new EventStore({
                    storageDirectory,
                    storageConfig: {
                        lock: EventStore.LOCK_RECLAIM
                    }
                });
                eventstore2.on('ready', () => {
                    expect(eventstore2.length).to.be(0);
                    expect(eventstore2.getStreamVersion('foo-bar')).to.be(0);
                    eventstore2.close();
                    done();
                });
            });
        });
    });

    it('throws when trying to open non-existing store read-only', function() {
        expect(() => new EventStore({
            storageDirectory,
            readOnly: true
        })).to.throwError();
    });

    it('can open read-only', function(done) {
        eventstore = new EventStore({
            storageDirectory
        });

        let events = [{foo: 'bar'}, {foo: 'baz'}, {foo: 'quux'}];
        eventstore.on('ready', () => {
            eventstore.commit('foo-bar', events, () => {
                let readstore = new EventStore({
                    storageDirectory,
                    readOnly: true
                });

                readstore.on('ready', () => {
                    const stream = readstore.getEventStream('foo-bar');
                    let i = 0;
                    for (let event of stream) {
                        expect(event).to.eql(events[i++]);
                    }
                    readstore.close();
                    done();
                });
            });
        });
    });

    describe('commit', function() {

        it('throws when no stream name specified', function() {
            eventstore = new EventStore({
                storageDirectory
            });

            expect(() => eventstore.commit({ foo: 'bar' })).to.throwError();
        });

        it('throws when no events specified', function() {
            eventstore = new EventStore({
                storageDirectory
            });

            expect(() => eventstore.commit('foo-bar')).to.throwError();
        });

        it('throws when opened in read-only mode', function() {
            eventstore = new EventStore({
                storageDirectory
            });
            eventstore.close();

            eventstore = new EventStore({
                storageDirectory,
                readOnly: true
            });

            expect(() => eventstore.commit('foo-bar', { foo: 'bar' })).to.throwError();
        });

        it('can commit a single event', function() {
            eventstore = new EventStore({
                storageDirectory
            });

            eventstore.commit('foo-bar', { foo: 'bar' });
            expect(eventstore.length).to.be(1);
        });

        it('can commit multiple events at once', function() {
            eventstore = new EventStore({
                storageDirectory
            });

            eventstore.commit('foo-bar', [{ foo: 'bar' }, { bar: 'baz' }, { baz: 'quux' }]);
            expect(eventstore.length).to.be(3);
        });

        it('invokes callback when finished', function(done) {
            eventstore = new EventStore({
                storageDirectory
            });

            eventstore.commit('foo-bar', [{ foo: 'bar' }], (commit) => {
                expect(eventstore.length).to.be(1);
                expect(commit.streamName).to.be('foo-bar');
                expect(commit.streamVersion).to.be(0);
                expect(commit.events).to.eql([{ foo: 'bar' }]);
                done();
            });
        });

        it('invokes callback when finished with optimistic concurrency check', function(done) {
            eventstore = new EventStore({
                storageDirectory
            });

            eventstore.commit('foo-bar', [{ foo: 'bar' }], EventStore.ExpectedVersion.EmptyStream, (commit) => {
                expect(eventstore.length).to.be(1);
                expect(commit.streamName).to.be('foo-bar');
                expect(commit.streamVersion).to.be(0);
                expect(commit.events).to.eql([{ foo: 'bar' }]);
                done();
            });
        });

        it('invokes callback when finished with optimistic concurrency check and metdata', function(done) {
            eventstore = new EventStore({
                storageDirectory
            });

            eventstore.commit('foo-bar', [{ foo: 'bar' }], EventStore.ExpectedVersion.EmptyStream, {}, (commit) => {
                expect(eventstore.length).to.be(1);
                expect(commit.streamName).to.be('foo-bar');
                expect(commit.streamVersion).to.be(0);
                expect(commit.events).to.eql([{ foo: 'bar' }]);
                done();
            });
        });

        it('invokes "commit" event when finished', function(done) {
            eventstore = new EventStore({
                storageDirectory
            });

            eventstore.on('commit', (commit) => {
                expect(eventstore.length).to.be(1);
                expect(commit.streamName).to.be('foo-bar');
                expect(commit.streamVersion).to.be(0);
                expect(commit.events).to.eql([{ foo: 'bar' }]);
                done();
            });
            eventstore.commit('foo-bar', [{ foo: 'bar' }]);
        });

        it('throws an optimistic concurrency error if stream version does not match', function(done) {
            eventstore = new EventStore({
                storageDirectory
            });

            expect(() => eventstore.commit('foo-bar', { foo: 'bar' }, 1)).to.throwError(
                e => expect(e).to.be.a(EventStore.OptimisticConcurrencyError)
            );

            eventstore.commit('foo-bar', { foo: 'bar' }, () => {
                expect(() => eventstore.commit('foo-bar', { foo: 'baz' }, EventStore.ExpectedVersion.EmptyStream)).to.throwError(
                    e => expect(e).to.be.a(EventStore.OptimisticConcurrencyError)
                );
                expect(() => eventstore.commit('foo-bar', { foo: 'baz' }, 2)).to.throwError(
                    e => expect(e).to.be.a(EventStore.OptimisticConcurrencyError)
                );
                done();
            });
        });

        it('does not throw an optimistic concurrency error if stream version matches', function(done) {
            eventstore = new EventStore({
                storageDirectory
            });

            expect(() => eventstore.commit('foo-bar', { foo: 'bar' }, EventStore.ExpectedVersion.EmptyStream)).to.not.throwError();

            eventstore.commit('foo-bar', { foo: 'bar' }, () => {
                expect(() => eventstore.commit('foo-bar', {foo: 'baz'}, 2)).to.not.throwError();
                expect(() => eventstore.commit('foo-bar', {foo: 'baz'}, EventStore.ExpectedVersion.Any)).to.not.throwError();
                done();
            });
        });

        it('uses metadata from argument for commit', function(done) {
            eventstore = new EventStore({
                storageDirectory
            });

            eventstore.commit('foo-bar', [{ foo: 'bar' }], { commitId: 1, committedAt: 12345, quux: 'quux' }, (commit) => {
                expect(commit.commitId).to.be(1);
                expect(commit.committedAt).to.be(12345);
                expect(commit.quux).to.be('quux');

                const stream = eventstore.getEventStream('foo-bar');
                const storedEvent = stream.next();
                expect(storedEvent.metadata.commitId).to.be(1);
                expect(storedEvent.metadata.committedAt).to.be(12345);
                expect(storedEvent.metadata.quux).to.be('quux');
                done();
            });
        });

    });

    describe('createEventStream', function() {

        it('throws when trying to recreate existing stream', function(done) {
            eventstore = new EventStore({
                storageDirectory
            });

            eventstore.commit('foo-bar', [{ type: 'foo' }], () => {
                expect(() => eventstore.createEventStream('foo-bar', event => event.payload.type === 'foo')).to.throwError();
                done();
            });
        });

        it('can create new streams on existing events', function(done) {
            eventstore = new EventStore({
                storageDirectory
            });

            eventstore.commit('foo-bar', [{ type: 'foo' }], () => {
                const stream = eventstore.createEventStream('my-foo-bar', event => event.payload.type === 'foo');
                expect(stream.events.length).to.be(1);
                expect(stream.events[0]).to.eql({ type: 'foo' });
                done();
            });
        });

    });

    describe('getStreamVersion', function() {

        it('returns -1 if the stream does not exist', function() {
            eventstore = new EventStore({
                storageDirectory
            });
            expect(eventstore.getStreamVersion('foo')).to.be(-1);
        });

        it('returns 0 if the stream is empty', function() {
            eventstore = new EventStore({
                storageDirectory
            });
            eventstore.createEventStream('foo', () => true);
            expect(eventstore.getStreamVersion('foo')).to.be(0);
        });

        it('returns the version of the stream', function(done) {
            eventstore = new EventStore({
                storageDirectory
            });
            eventstore.commit('foo', [{ type: 'foo' }], () => {
                expect(eventstore.getStreamVersion('foo')).to.be(1);
                done();
            });
        });

    });

    describe('getEventStream', function() {

        it('can open existing streams', function(done) {
            eventstore = new EventStore({
                storageDirectory
            });

            eventstore.commit('foo-bar', [{ foo: 'bar' }]);
            eventstore.close();

            eventstore = new EventStore({
                storageDirectory
            });
            eventstore.on('ready', () => {
                const stream = eventstore.getEventStream('foo-bar');
                expect(stream.events.length).to.be(1);
                done();
            });
        });

        it('can iterate events in reverse order', function() {
            eventstore = new EventStore({
                storageDirectory
            });

            for (let i=1; i<=20; i++) {
                eventstore.commit('foo-bar', [{key: i}]);
            }

            let reverseStream = eventstore.getEventStream('foo-bar', -1, 0);
            let i = 20;
            for (let event of reverseStream) {
                expect(event).to.eql({ key: i-- });
            }
        });

        it('can open streams created in writer', function(done) {
            eventstore = new EventStore({
                storageDirectory
            });

            const readstore = new EventStore({
                storageDirectory,
                readOnly: true
            });

            expect(readstore.getStreamVersion('foo')).to.be(-1);

            readstore.on('stream-available', (streamName) => {
                if (streamName === 'foo') {
                    expect(readstore.getStreamVersion('foo')).to.be(0);
                    readstore.close();
                    done();
                }
            });

            eventstore.createEventStream('foo', { type: 'foo' });
        });

        it('needs to be tested further.');
    });

    describe('getAllEvents', function() {

        it('returns stream for all events', function (done) {
            eventstore = new EventStore({
                storageDirectory
            });

            for (let i=1; i<=20; i++) {
                eventstore.commit('foo-bar', [{key: i}]);
            }

            eventstore.on('ready', () => {
                const stream = eventstore.getAllEvents();
                expect(stream.events.length).to.be(20);
                done();
            });
        });

    });

    describe('fromStreams', function() {

        it('throws when not specifying a join stream name', function() {
            eventstore = new EventStore({
                storageDirectory
            });

            expect(() => eventstore.fromStreams()).to.throwError();
        });

        it('throws when not specifying an array of stream names to join', function() {
            eventstore = new EventStore({
                storageDirectory
            });

            expect(() => eventstore.fromStreams('join-foo-bar')).to.throwError();
        });

        it('throws when specifying a non-existing stream to join', function() {
            eventstore = new EventStore({
                storageDirectory
            });

            expect(() => eventstore.fromStreams('join-foo-bar', ['foo-bar', 'baz'])).to.throwError(/does not exist/);
        });

        it('iterates events from multiple streams in correct order', function(done) {
            eventstore = new EventStore({
                storageDirectory
            });

            eventstore.commit('foo', { key: 1 }, () => {
                eventstore.commit('bar', { key: 2}, () => {
                    eventstore.commit('foo', { key: 3 }, () => {
                        eventstore.commit('bar', { key: 4}, () => {
                            let joinStream = eventstore.fromStreams('foobar', ['foo','bar']);
                            let key = 1;
                            for (let event of joinStream) {
                                expect(event.key).to.be(key);
                                key++;
                            }
                            expect(key).to.be(5);
                            done();
                        });
                    });
                });
            });
        });

        it('iterates events from multiple streams in reverse order', function() {
            eventstore = new EventStore({
                storageDirectory
            });

            for (let i=1; i<=20; i++) {
                eventstore.commit(i % 2 ? 'foo' : 'bar', [{key: i}]);
            }

            let reverseStream = eventstore.fromStreams('foo-bar', ['foo', 'bar'],-1, 0);
            let i = 20;
            for (let event of reverseStream) {
                expect(event).to.eql({ key: i-- });
            }
            expect(i).to.be(0);
        });

    });

    describe('getEventStreamForCategory', function() {

        it('throws when not specifying category without streams', function () {
            eventstore = new EventStore({
                storageDirectory
            });

            expect(() => eventstore.getEventStreamForCategory('non-existing-category')).to.throwError();
        });

        it('iterates events for all streams with a given category prefix', function () {
            eventstore = new EventStore({
                storageDirectory
            });

            eventstore.commit('bar', [{key: 0}]);
            for (let i=1; i<=20; i++) {
                eventstore.commit('foo-' + i, [{key: i}]);
            }
            eventstore.commit('foobar', [{key: 21}]);

            let categoryStream = eventstore.getEventStreamForCategory('foo');
            let i = 1;
            for (let event of categoryStream) {
                expect(event).to.eql({ key: i++ });
            }
            expect(i).to.be(21);
        });

        it('works with a dedicated stream for the category', function () {
            eventstore = new EventStore({
                storageDirectory
            });

            eventstore.createEventStream('foo', e => e.stream.startsWith('foo-'));
            for (let i=1; i<=20; i++) {
                eventstore.commit('foo-' + i, [{key: i}]);
            }

            let categoryStream = eventstore.getEventStreamForCategory('foo');
            let i = 1;
            for (let event of categoryStream) {
                expect(event).to.eql({ key: i++ });
            }
            expect(i).to.be(21);
        });

    });

    describe('createEventStream', function() {

        it('throws in read-only mode', function () {
            eventstore = new EventStore({
                storageDirectory
            });

            let readstore = new EventStore({
                storageDirectory,
                readOnly: true
            });
            expect(() => readstore.createEventStream('foo-bar', () => true)).to.throwError();
            readstore.close();
        });

        it('throws when trying to re-create stream', function () {
            eventstore = new EventStore({
                storageDirectory
            });
            eventstore.createEventStream('foo-bar', () => true)

            expect(() => eventstore.createEventStream('foo-bar', () => true)).to.throwError();
        });
    });

    describe('deleteEventStream', function() {

        it('throws in read-only mode', function(done) {
            eventstore = new EventStore({
                storageDirectory
            });
            eventstore.createEventStream('foo-bar', () => true);

            let readstore = new EventStore({
                storageDirectory,
                readOnly: true
            });
            readstore.on('ready', () => {
                expect(() => readstore.deleteEventStream('foo-bar')).to.throwError();
                readstore.close();
                done();
            });
        });

        it('removes the stream persistently', function(done) {
            eventstore = new EventStore({
                storageDirectory
            });

            eventstore.commit('foo-bar', [{ foo: 'bar' }], () => {
                expect(fs.existsSync(storageDirectory + '/streams/eventstore.stream-foo-bar.index')).to.be(true);
                eventstore.deleteEventStream('foo-bar');
                expect(eventstore.getEventStream('foo-bar')).to.be(false);
                expect(fs.existsSync(storageDirectory + '/streams/eventstore.stream-foo-bar.index')).to.be(false);
                done();
            });
        });

        it('is noop for non-existing stream', function(done) {
            eventstore = new EventStore({
                storageDirectory
            });

            eventstore.commit('foo-bar', [{ foo: 'bar' }], () => {
                eventstore.deleteEventStream('bar');
                expect(eventstore.getEventStream('foo-bar')).to.not.be(false);
                done();
            });
        });

    });

    describe('getConsumer', function() {

        it('returns a consumer for the given stream', function(done) {
            eventstore = new EventStore({
                storageDirectory
            });
            eventstore.createEventStream('foo-bar', event => event.payload.foo === 'bar');

            const consumer = eventstore.getConsumer('foo-bar', 'consumer1');
            consumer.on('data', event => {
                expect(event.id).to.be(2);
                done();
            });
            eventstore.commit('foo', { foo: 'baz', id: 1 });
            eventstore.commit('foo', { foo: 'bar', id: 2 });
        });

    });

});
