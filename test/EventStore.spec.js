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

    it('needs to be tested.');

});
