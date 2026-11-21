import expect from 'expect.js';
import fs from 'fs-extra';
import { fileURLToPath } from 'url';
import EventStore from '../src/EventStore.js';
import { localSource } from '../src/LocalSource.js';

const __dirname = fileURLToPath(new URL('.', import.meta.url));
const storageDirectory = __dirname + 'data';

const order = (id, amount) => ({ type: 'OrderPlaced', id, amount, tags: [`order/${id}`] });

describe('localSource', function () {

    let store, source;

    beforeEach(function (done) {
        fs.emptyDirSync(storageDirectory);
        store = new EventStore({ storageDirectory, typeAccessor: 'type', tagsAccessor: 'tags' });
        source = localSource(store);
        store.once('ready', done);
    });

    afterEach(function () {
        store.close();
    });

    it('delivers envelopes with position, type and tags', async function () {
        store.commit('orders', [order('o1', 5), order('o2', 7)]);
        const { events, condition } = await source.query({ items: [{ types: ['OrderPlaced'] }] });
        const envelopes = [];
        for await (const envelope of events) envelopes.push(envelope);
        expect(envelopes.map(e => [e.position, e.type, e.tags])).to.eql([[1, 'OrderPlaced', ['order/o1']], [2, 'OrderPlaced', ['order/o2']]]);
        expect(envelopes[0].metadata.commitId).to.be(0);
        expect(condition.noneMatchAfter).to.be(2);
    });

    it('stops a query at the position of its condition', async function () {
        store.commit('orders', [order('o1', 5)]);
        const { events, condition } = await source.query({ items: [{ types: ['OrderPlaced'] }] });
        store.commit('orders', [order('o2', 7)]);
        const positions = [];
        for await (const { position } of events) positions.push(position);
        expect(positions).to.eql([1]);
        expect(condition.noneMatchAfter).to.be(1);
    });

    it('reads a selection with a matcher from a position', async function () {
        store.commit('orders', [order('o1', 5), order('o2', 7), order('o3', 9)]);
        const { condition: { selector } } = await source.query({ items: [{ types: ['OrderPlaced'] }] });
        const positions = [];
        for await (const { position } of source.read({ selector, matcher: { payload: { amount: { $gt: 5 } } } }, { from: 2 })) {
            positions.push(position);
        }
        expect(positions).to.eql([2, 3]);
    });

    it('follows new events of a selection until aborted', async function () {
        store.commit('orders', [order('o1', 5)]);
        const abort = new AbortController();
        const { condition } = await source.query({ items: [{ tags: ['order/o2'] }] });
        const seen = [];
        const following = (async () => {
            for await (const { event, position } of source.follow(condition, { from: 1, signal: abort.signal })) {
                seen.push([position, event.id]);
                if (seen.length === 2) abort.abort();
            }
        })();
        store.commit('orders', [order('o3', 1), order('o2', 1)]);
        store.commit('orders', [{ type: 'OrderShipped', id: 'o2', tags: ['order/o2'] }]);
        await following;
        expect(seen).to.eql([[3, 'o2'], [4, 'o2']]);
        store.commit('orders', [order('o2', 2)]);
    });

    it('applies a function matcher while following', async function () {
        const abort = new AbortController();
        const matcher = (event) => event.amount > 5;
        const seen = [];
        const following = (async () => {
            for await (const { event } of source.follow({ selector: ['_all'], matcher }, { signal: abort.signal })) {
                seen.push(event.id);
                abort.abort();
            }
        })();
        store.commit('orders', [order('o1', 5), order('o2', 7)]);
        await following;
        expect(seen).to.eql(['o2']);
    });

    it('ends when aborted while waiting', async function () {
        const abort = new AbortController();
        const following = (async () => {
            for await (const envelope of source.follow({ selector: ['_all'] }, { signal: abort.signal })) {
                throw new Error('unexpected ' + envelope.position);
            }
        })();
        await new Promise(resolve => setImmediate(resolve));
        abort.abort();
        await following;
        expect(store.storage.listenerCount('index-add')).to.be(0);
    });
});
