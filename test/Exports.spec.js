import expect from 'expect.js';
import EventStore, { VERSION, localSource, fileSnapshotStore } from '../index.js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

describe('Public exports', function() {
    it('exports VERSION and mirrors it on EventStore.VERSION', function() {
        expect(VERSION).to.be.a('string');
        expect(VERSION.length > 0).to.be(true);
        expect(EventStore.VERSION).to.be(VERSION);
    });

    it('exports localSource and fileSnapshotStore', function() {
        expect(localSource).to.be.a('function');
        expect(fileSnapshotStore).to.be.a('function');
    });
});


describe('Projections subpath export', function() {
    it('exposes the projection API under event-storage/projections', async function() {
        const projections = await import('event-storage/projections');
        expect(projections.createProjection).to.be.a('function');
        expect(projections.composeProjections).to.be.a('function');
        expect(projections.buildDecisionModel).to.be.a('function');
        expect(projections.DecisionModel).to.be.a('function');
        expect(projections.createReadModel).to.be.a('function');
        expect(projections.ReadModel).to.be.a('function');
        expect(projections.transformEvents).to.be.a('function');
    });

    it('only imports events from node, so it can run in a bundle outside of node', function() {
        const root = fileURLToPath(new URL('../', import.meta.url));
        const seen = new Set();
        const builtins = [];
        const visit = (file) => {
            if (seen.has(file)) return;
            seen.add(file);
            const source = fs.readFileSync(file, 'utf8');
            for (const [, specifier] of source.matchAll(/^(?:import|export)[^'"]*from\s+['"]([^'"]+)['"]/gm)) {
                if (specifier.startsWith('.')) visit(path.resolve(path.dirname(file), specifier));
                else builtins.push(specifier);
            }
        };
        visit(path.join(root, 'projections.js'));
        expect(builtins).to.eql(['events']);
    });
});
