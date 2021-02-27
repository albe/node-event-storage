const Benchmark = require('benchmark');
const benchmarks = require('beautify-benchmark');
const fs = require('fs-extra');

const Suite = new Benchmark.Suite('partition');
Suite.on('cycle', () => fs.emptyDirSync('data'));
Suite.on('cycle', (event) => benchmarks.add(event.target));
Suite.on('complete', () => benchmarks.log());
Suite.on('error', (e) => console.log(e.target.error));

const Stable = require('event-storage');
const Latest = require('../index');

const WRITES = 1000;

function bench(storage) {
	for (let i = 1; i<=WRITES; i++) {
		storage.write({ doc: 'this is some test document for measuring performance', value: 123.45, amount: 999, number: i });
	}
	storage.close();

	let number;
	storage.open();
	for (let doc of storage.readRange(1)) {
		number = doc.number;
	}
	storage.close();
	if (number !== WRITES) throw new Error('Not all documents were written! Last document was '+number);
}

Suite.add('storage [stable]', () => {
	bench(new Stable.Storage('storage', { dataDirectory: 'data' }));
});

Suite.add('storage [latest]', () => {
	bench(new Latest.Storage('storage', { dataDirectory: 'data' }));
});

Suite.run();