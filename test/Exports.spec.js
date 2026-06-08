import expect from 'expect.js';
import EventStore, { VERSION } from '../index.js';

describe('Public exports', function() {
    it('exports VERSION and mirrors it on EventStore.VERSION', function() {
        expect(VERSION).to.be.a('string');
        expect(VERSION.length > 0).to.be(true);
        expect(EventStore.VERSION).to.be(VERSION);
    });
});

