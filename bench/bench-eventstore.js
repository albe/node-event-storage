const Benchmark = require('benchmark');
const benchmarks = require('beautify-benchmark');
const fs = require('fs-extra');

const Suite = new Benchmark.Suite('eventstore');
Suite.on('start', () => fs.emptyDirSync('data'));
Suite.on('cycle', (event) => benchmarks.add(event.target));
Suite.on('complete', () => benchmarks.log());
Suite.on('error', (e) => console.log(e.target.error));

const Stable = require('event-storage');
const Latest = require('../index');

const WRITES = 1000;

/**
 * @param {Stable|Latest} store
 */
function bench(store, cycle) {
	const streamName = 'test-stream-'+cycle;
	for (let i = 1; i<=WRITES; i++) {
		store.commit(streamName, { doc: 'this is some test document for measuring performance', value: 123.45, amount: 999, number: i });
	}

	let number = 0;
	for (let doc of store.getEventStream(streamName, -WRITES)) {
		number++;
	}
	store.close();
	if (number < WRITES) throw new Error('Not all documents were written! Last document was '+number);
}

Suite.add('eventstore [stable]', function() {
	bench(new Stable('eventstore', { storageDirectory: 'data/stable' }), this.cycles);
});

Suite.add('eventstore [latest]', function() {
	bench(new Latest('eventstore', { storageDirectory: 'data/latest' }), this.cycles);
});

Suite.run();