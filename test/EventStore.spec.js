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
        eventstore = undefined;
    });

    it('basically works', function(done) {
        eventstore = new EventStore({
            storageDirectory: 'test/data'
        });

        let events = [{foo: 'bar'}, {foo: 'baz'}, {foo: 'quux'}];
        eventstore.on('ready', () => {
            eventstore.commit('foo-bar', events, () => {
                let stream = eventstore.getEventStream('foo-bar');
                let i = 0;
                for (let event of stream) {
                    expect(event).to.eql(events[i++]);
                }
                done();
            });
        });
    });

    describe('commit', function() {

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

        it('uses metadata from argument for commit', function() {
            eventstore = new EventStore({
                storageDirectory: 'test/data'
            });

            eventstore.commit('foo-bar', [{ foo: 'bar' }], { commitId: 1, committedAt: 12345, quux: 'quux' }, (commit) => {
                expect(commit.commitId).to.be(1);
                expect(commit.committedAt).to.be(12345);
                expect(commit.quux).to.be('quux');

                let stream = eventstore.getEventStream('foo-bar');
                let storedEvent = stream.next().value;
                expect(storedEvent.metadata.commitId).to.be(1);
                expect(storedEvent.metadata.committedAt).to.be(12345);
                expect(storedEvent.metadata.quux).to.be('quux');
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

            let commits = eventstore.getCommits(0);
            expect(commits.next()).to.eql({ value: undefined, done: true });
        });

        it('returns a list of all commits', function(done) {
            eventstore = new EventStore({
                storageDirectory: 'test/data'
            });
            let events = [
                [{ foo: 1 }, { foo: 2 }],
                [{ bar: 1 }],
                [{ baz: 1 }, { baz: 2 }, { baz: 3 }]
            ];
            commitAll('foo-bar', events, () => {
                let commits = eventstore.getCommits(0);
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
            let events = [
                [{ foo: 1 }, { foo: 2 }],
                [{ bar: 1 }],
                [{ baz: 1 }, { baz: 2 }, { baz: 3 }]
            ];
            commitAll('foo-bar', events, () => {
                let commits = eventstore.getCommits(4);
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
            let events = [
                [{ foo: 1 }, { foo: 2 }],
                [{ bar: 1 }],
                [{ baz: 1 }, { baz: 2 }, { baz: 3 }]
            ];
            commitAll('foo-bar', events, () => {
                let commits = eventstore.getCommits(5);
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

    it('needs to be tested.');

});
