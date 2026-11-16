import expect from 'expect.js';
import EventStore, { VERSION } from '../index.js';

describe('Public exports', function() {
    it('exports VERSION and mirrors it on EventStore.VERSION', function() {
        expect(VERSION).to.be.a('string');
        expect(VERSION.length > 0).to.be(true);
        expect(EventStore.VERSION).to.be(VERSION);
    });
});


describe('Projections subpath export', function() {
    it('exposes the projection API under event-storage/projections', async function() {
        const projections = await import('event-storage/projections');
        expect(projections.createProjection).to.be.a('function');
        expect(projections.composeProjections).to.be.a('function');
        expect(projections.buildDecisionModel).to.be.a('function');
    });
});
