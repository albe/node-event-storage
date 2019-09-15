const expect = require('expect.js');
const fs = require('fs-extra');
const EventStore = require('../src/EventStore');

describe('EventStore', function() {

    let eventstore;

    beforeEach(function () {
        fs.emptyDirSync('test/data');
    });

    afterEach(function () {
        if (eventstore) eventstore.close();
        eventstore = null;
    });

    it('basically works', function(done) {
        eventstore = new EventStore({
            storageDirectory: 'test/data'
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

    describe('commit', function() {

        it('throws when no stream name specified', function() {
            eventstore = new EventStore({
                storageDirectory: 'test/data'
            });

            expect(() => eventstore.commit({ foo: 'bar' })).to.throwError();
        });

        it('throws when no events specified', function() {
            eventstore = new EventStore({
                storageDirectory: 'test/data'
            });

            expect(() => eventstore.commit('foo-bar')).to.throwError();
        });

        it('can commit a single event', function() {
            eventstore = new EventStore({
                storageDirectory: 'test/data'
            });

            eventstore.commit('foo-bar', { foo: 'bar' });
            expect(eventstore.length).to.be(1);
        });

        it('can commit multiple events at once', function() {
            eventstore = new EventStore({
                storageDirectory: 'test/data'
            });

            eventstore.commit('foo-bar', [{ foo: 'bar' }, { bar: 'baz' }, { baz: 'quux' }]);
            expect(eventstore.length).to.be(3);
        });

        it('invokes callback when finished', function(done) {
            eventstore = new EventStore({
                storageDirectory: 'test/data'
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
                storageDirectory: 'test/data'
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
                storageDirectory: 'test/data'
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
                storageDirectory: 'test/data'
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
                storageDirectory: 'test/data'
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
                storageDirectory: 'test/data'
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
                storageDirectory: 'test/data'
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
                storageDirectory: 'test/data'
            });

            eventstore.commit('foo-bar', [{ type: 'foo' }], () => {
                expect(() => eventstore.createEventStream('foo-bar', event => event.payload.type === 'foo')).to.throwError();
                done();
            });
        });

        it('can create new streams on existing events', function(done) {
            eventstore = new EventStore({
                storageDirectory: 'test/data'
            });

            eventstore.commit('foo-bar', [{ type: 'foo' }], () => {
                const stream = eventstore.createEventStream('my-foo-bar', event => event.payload.type === 'foo');
                expect(stream.events.length).to.be(1);
                expect(stream.events[0]).to.eql({ type: 'foo' });
                done();
            });
        });

    });

    describe('getEventStream', function() {

        it('can open existing streams', function(done) {
            eventstore = new EventStore({
                storageDirectory: 'test/data'
            });

            eventstore.commit('foo-bar', [{ foo: 'bar' }]);
            eventstore.close();

            eventstore = new EventStore({
                storageDirectory: 'test/data'
            });
            eventstore.on('ready', () => {
                const stream = eventstore.getEventStream('foo-bar');
                expect(stream.events.length).to.be(1);
                done();
            });
        });

        it('needs to be tested.');
    });

    describe('fromStreams', function() {

        it('throws when not specifying a join stream name', function() {
            eventstore = new EventStore({
                storageDirectory: 'test/data'
            });

            expect(() => eventstore.fromStreams()).to.throwError();
        });

        it('throws when not specifying an array of stream names to join', function() {
            eventstore = new EventStore({
                storageDirectory: 'test/data'
            });

            expect(() => eventstore.fromStreams('join-foo-bar')).to.throwError();
        });

        it('throws when specifying a non-existing stream to join', function() {
            eventstore = new EventStore({
                storageDirectory: 'test/data'
            });

            expect(() => eventstore.fromStreams('join-foo-bar', ['foo-bar', 'baz'])).to.throwError(/does not exist/);
        });

        it('needs to be tested.');
    });

    describe('deleteEventStream', function() {

        it('removes the stream persistently', function(done) {
            eventstore = new EventStore({
                storageDirectory: 'test/data'
            });

            eventstore.commit('foo-bar', [{ foo: 'bar' }], () => {
                expect(fs.existsSync('test/data/streams/eventstore.stream-foo-bar.index')).to.be(true);
                eventstore.deleteEventStream('foo-bar');
                expect(eventstore.getEventStream('foo-bar')).to.be(false);
                expect(fs.existsSync('test/data/streams/eventstore.stream-foo-bar.index')).to.be(false);
                done();
            });
        });

        it('is noop for non-existing stream', function(done) {
            eventstore = new EventStore({
                storageDirectory: 'test/data'
            });

            eventstore.commit('foo-bar', [{ foo: 'bar' }], () => {
                eventstore.deleteEventStream('bar');
                expect(eventstore.getEventStream('foo-bar')).to.not.be(false);
                done();
            });
        });

    });

    describe('getCommits', function() {

        function commitAll(streamName, commits, callback) {
            for (let i = 0; i < commits.length; i++) {
                eventstore.commit(streamName, commits[i], i === commits.length - 1 ? callback : undefined);
            }
        }

        it('returns empty iterator if no commits in store', function() {
            eventstore = new EventStore({
                storageDirectory: 'test/data'
            });

            const commits = eventstore.getCommits(0);
            expect(commits.next()).to.eql({ value: undefined, done: true });
        });

        it('returns a list of all commits', function(done) {
            eventstore = new EventStore({
                storageDirectory: 'test/data'
            });
            const events = [
                [{ foo: 1 }, { foo: 2 }],
                [{ bar: 1 }],
                [{ baz: 1 }, { baz: 2 }, { baz: 3 }]
            ];
            commitAll('foo-bar', events, () => {
                const commits = eventstore.getCommits(0);
                let streamVersion = 0;
                let commitNumber = 0;
                for (let commit of commits) {
                    expect(commit.streamName).to.be('foo-bar');
                    expect(commit.streamVersion).to.be(streamVersion);
                    expect(commit.events).to.eql(events[commitNumber++]);
                    streamVersion += commit.events.length;
                }
                expect(commitNumber).to.be(3);
                done();
            });
        });

        it('returns only commits after the given revision', function(done) {
            eventstore = new EventStore({
                storageDirectory: 'test/data'
            });
            const events = [
                [{ foo: 1 }, { foo: 2 }],
                [{ bar: 1 }],
                [{ baz: 1 }, { baz: 2 }, { baz: 3 }]
            ];
            commitAll('foo-bar', events, () => {
                const commits = eventstore.getCommits(4);
                let streamVersion = 3;
                let commitNumber = 2;
                for (let commit of commits) {
                    expect(commit.streamName).to.be('foo-bar');
                    expect(commit.streamVersion).to.be(streamVersion);
                    expect(commit.events).to.eql(events[commitNumber++]);
                    streamVersion += commit.events.length;
                }
                expect(commitNumber).to.be(3);
                done();
            });
        });

        it('returns the full commit if given revision is within a single commit', function(done) {
            eventstore = new EventStore({
                storageDirectory: 'test/data'
            });
            const events = [
                [{ foo: 1 }, { foo: 2 }],
                [{ bar: 1 }],
                [{ baz: 1 }, { baz: 2 }, { baz: 3 }]
            ];
            commitAll('foo-bar', events, () => {
                const commits = eventstore.getCommits(5);
                let streamVersion = 3;
                let commitNumber = 2;
                for (let commit of commits) {
                    expect(commit.streamName).to.be('foo-bar');
                    expect(commit.streamVersion).to.be(streamVersion);
                    expect(commit.events).to.eql(events[commitNumber++]);
                    streamVersion += commit.events.length;
                }
                expect(commitNumber).to.be(3);
                done();
            });
        });

    });

    describe('getConsumer', function() {

        it('returns a consumer for the given stream', function(done) {
            eventstore = new EventStore({
                storageDirectory: 'test/data'
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
