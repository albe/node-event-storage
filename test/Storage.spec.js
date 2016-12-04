const expect = require('expect.js');
const fs = require('fs-extra');
const Storage = require('../src/Storage');

describe('storage', function() {

    let storage;

    beforeEach(function () {
        fs.emptyDirSync('test/data');
    });

    afterEach(function () {
        if (storage) storage.close();
        storage = undefined;
    });

    it('needs to be tested.');
});
