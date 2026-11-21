import expect from 'expect.js';
import fs from 'fs-extra';
import { fileURLToPath } from 'url';
import EventStore from '../src/EventStore.js';

const __dirname = fileURLToPath(new URL('.', import.meta.url));
const storageDirectory = __dirname + 'data';

function open(config) {
    return new Promise(resolve => {
        const store = new EventStore({ storageDirectory, ...config });
        store.once('ready', () => resolve(store));
    });
}

function commit(store, stream, events) {
    return new Promise(resolve => store.commit(stream, events, resolve));
}

describe('Type stream migration', function () {

    let store;

    beforeEach(function () {
        fs.emptyDirSync(storageDirectory);
    });

    afterEach(function () {
        store?.close();
        store = null;
    });

    // Before type streams were namespaced, typeAccessor created streams named like the type.
    const legacy = { streamSources: [{ path: 'type', nameBuilder: (type) => type }] };

    it('moves legacy type streams to types/<type> with all their events', async function () {
        store = await open(legacy);
        await commit(store, 'orders', [{ type: 'OrderPlaced', id: 1 }, { type: 'OrderShipped', id: 1 }]);
        await commit(store, 'orders', [{ type: 'OrderPlaced', id: 2 }]);
        expect(store.getStreamVersion('OrderPlaced')).to.be(2);
        store.close();

        const migrated = [];
        store = new EventStore({ storageDirectory, typeAccessor: 'type' });
        store.on('stream-migrated', (from, to) => migrated.push([from, to]));
        await new Promise(resolve => store.once('ready', resolve));

        expect(migrated).to.eql([['OrderPlaced', 'types/OrderPlaced'], ['OrderShipped', 'types/OrderShipped']]);
        expect(store.getStreamVersion('OrderPlaced')).to.be(-1);
        expect(store.getStreamVersion('types/OrderPlaced')).to.be(2);
        expect(store.query({ items: [{ types: ['OrderPlaced'] }] }).stream.events.map(e => e.id)).to.eql([1, 2]);
    });

    it('keeps indexing new events into the migrated stream and stays migrated after a restart', async function () {
        store = await open(legacy);
        await commit(store, 'orders', [{ type: 'OrderPlaced', id: 1 }]);
        store.close();

        store = await open({ typeAccessor: 'type' });
        await commit(store, 'orders', [{ type: 'OrderPlaced', id: 2 }]);
        expect(store.getStreamVersion('types/OrderPlaced')).to.be(2);
        store.close();

        store = await open({ typeAccessor: 'type' });
        expect(store.getStreamVersion('OrderPlaced')).to.be(-1);
        expect(store.query({ items: [{ types: ['OrderPlaced'] }] }).stream.events.map(e => e.id)).to.eql([1, 2]);
    });

    it('leaves streams alone that only share the name of a type', async function () {
        store = await open({});
        await commit(store, 'OrderPlaced', [{ type: 'Something', id: 1 }]);
        store.close();

        store = await open({ typeAccessor: 'type' });
        expect(store.getStreamVersion('OrderPlaced')).to.be(1);
        expect(store.getStreamVersion('types/OrderPlaced')).to.be(-1);
    });

    it('does not migrate in a read-only store', async function () {
        store = await open(legacy);
        await commit(store, 'orders', [{ type: 'OrderPlaced', id: 1 }]);
        store.close();

        store = await open({ typeAccessor: 'type', readOnly: true });
        expect(store.getStreamVersion('OrderPlaced')).to.be(1);
        expect(store.getStreamVersion('types/OrderPlaced')).to.be(-1);
    });
});
