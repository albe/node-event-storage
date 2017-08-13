const expect = require('expect.js');
const fs = require('fs-extra');
const JoinEventStream = require('../src/JoinEventStream');
const EventStore = require('../src/EventStore');

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
        eventstore = undefined;
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
        stream = new JoinEventStream('foo-bar', ['foo', 'bar'], eventstore, 0, 1);
        const fetchedEvents = stream.events;

        expect(fetchedEvents.length).to.be(2);
        expect(fetchedEvents[0]).to.eql(events[0]);
        expect(fetchedEvents[1]).to.eql(events[1]);
    });

    it('can fetch events from the end only', function(){
        stream = new JoinEventStream('foo-bar', ['foo', 'bar'], eventstore, -2, -1);
        const fetchedEvents = stream.events;

        expect(fetchedEvents.length).to.be(2);
        expect(fetchedEvents[0]).to.eql(events[1]);
        expect(fetchedEvents[1]).to.eql(events[2]);
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

});
