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

    it('needs to be tested.');

});
