const Benchmark = require('benchmark');
const benchmarks = require('beautify-benchmark');
const fs = require('fs-extra');

const Suite = new Benchmark.Suite('eventstore');
Suite.on('cycle', () => fs.emptyDirSync('data'));
Suite.on('cycle', (event) => benchmarks.add(event.target));
Suite.on('complete', () => benchmarks.log());
Suite.on('error', (e) => console.log(e.target.error));

const Stable = require('event-storage');
const Latest = require('../index');

const WRITES = 1000;

/**
 * @param {Stable|Latest} store
 */
function bench(store) {
	for (let i = 1; i<=WRITES; i++) {
		store.commit('test-stream', { doc: 'this is some test document for measuring performance', value: 123.45, amount: 999, number: i });
	}

	let number;
	for (let doc of store.getEventStream('test-stream')) {
		number = doc.number;
	}
	store.close();
	if (number !== WRITES) throw new Error('Not all documents were written! Last document was '+number);
}

Suite.add('storage [stable]', () => {
	bench(new Stable('eventstore', { storageDirectory: 'data' }));
});

Suite.add('storage [latest]', () => {
	bench(new Latest('eventstore', { storageDirectory: 'data' }));
});

Suite.run();