const Benchmark = require('benchmark');
const benchmarks = require('beautify-benchmark');
const fs = require('fs-extra');

const Suite = new Benchmark.Suite('read-scenarios');
Suite.on('cycle', (event) => benchmarks.add(event.target));
Suite.on('complete', () => { benchmarks.log(); process.exit(0); });
Suite.on('error', (e) => console.log(e.target.error));

const Stable = require('event-storage');
const Latest = require('../index');

const EVENTS = 10000;
const STREAM_1 = 'stream-1';
const STREAM_2 = 'stream-2';
// Keep each event document close to 512 bytes
const EVENT_DOC = { type: 'SomeEvent', payload: 'x'.repeat(460) };

function countAll(iter) {
    let n = 0;
    for (const _ of iter) n++; // jshint ignore:line
    return n;
}

function populateStore(EventStore, directory) {
    return new Promise((resolve, reject) => {
        fs.emptyDirSync(directory);
        const store = new EventStore('bench', { storageDirectory: directory });
        store.once('ready', () => {
            for (let i = 0; i < EVENTS; i++) {
                store.commit(i % 2 === 0 ? STREAM_1 : STREAM_2, Object.assign({ seq: i }, EVENT_DOC));
            }
            store.close();
            resolve();
        });
        store.once('error', reject);
    });
}

function openReadOnly(EventStore, directory) {
    return new Promise((resolve, reject) => {
        const store = new EventStore('bench', { storageDirectory: directory, readOnly: true });
        store.once('ready', () => resolve(store));
        store.once('error', reject);
    });
}

populateStore(Stable, 'data/stable')
    .then(() => populateStore(Latest, 'data/latest'))
    .then(() => Promise.all([
        openReadOnly(Stable, 'data/stable'),
        openReadOnly(Latest, 'data/latest'),
    ]))
    .then(([stableStore, latestStore]) => {
        const third = Math.ceil(EVENTS / 3);
        const twoThirds = Math.floor(2 * EVENTS / 3);

        Suite
            .add('1 - forward full scan [stable]',   () => countAll(stableStore.getAllEvents()))
            .add('1 - forward full scan [latest]',   () => countAll(latestStore.getAllEvents()))
            .add('2 - backwards full scan [stable]', () => countAll(stableStore.getAllEvents(-1, 1)))
            .add('2 - backwards full scan [latest]', () => countAll(latestStore.getAllEvents(-1, 1)))
            .add('3 - join stream [stable]',         () => countAll(stableStore.fromStreams('join', [STREAM_1, STREAM_2])))
            .add('3 - join stream [latest]',         () => countAll(latestStore.fromStreams('join', [STREAM_1, STREAM_2])))
            .add('4 - range scan [stable]',          () => countAll(stableStore.getAllEvents(third, twoThirds)))
            .add('4 - range scan [latest]',          () => countAll(latestStore.getAllEvents(third, twoThirds)))
            .run();
    })
    .catch((e) => { console.error(e); process.exit(1); });
