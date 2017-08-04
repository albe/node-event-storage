const expect = require('expect.js');
const EventStream = require('../src/EventStream');

describe('EventStream', function() {

    let stream, mockEventStore;
    const events = ['foo', 'bar', 'baz'];

    beforeEach(function () {
        // This is pretty ugly testing internals, but that's only because we extracted this ugliness out of the EventStore
        mockEventStore = {
            streams: {
                'foo': {
                    index: 'foo-index'
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

    it('throws if no name specified in constructor', function(){
        expect(() => new EventStream()).to.throwError();
    });

    it('throws if no EventStore specified in constructor', function(){
        expect(() => new EventStream('foo')).to.throwError();
    });

    it('makes all events accessible as array', function(){
        expect(stream.events).to.eql(events);
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

    it('adjusts revisions to 1-based index', function(){
        stream = new EventStream('foo', mockEventStore, 0, 1);
        const events = stream.events;

        expect(mockEventStore.storage.from).to.be(1);
        expect(mockEventStore.storage.until).to.be(2);
    });

    it('leaves negative revisions untouched', function(){
        stream = new EventStream('foo', mockEventStore, -1, -1);
        const events = stream.events;

        expect(mockEventStore.storage.from).to.be(-1);
        expect(mockEventStore.storage.until).to.be(-1);
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

});
