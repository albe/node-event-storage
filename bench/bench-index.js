const Benchmark = require('benchmark');
const benchmarks = require('beautify-benchmark');
const fs = require('fs');

const Suite = new Benchmark.Suite('index');
Suite.on('cycle', () => fs.existsSync('data/.index') && fs.unlinkSync('data/.index'));
Suite.on('cycle', (event) => benchmarks.add(event.target));
Suite.on('complete', () => benchmarks.log());
Suite.on('error', (e) => console.log(e));

const Stable = require('event-storage');
const Latest = require('../index');

const WRITES = 1000;

Suite.add('index [stable]', () => {
	const index = new Stable.Index('.index', { dataDirectory: 'data' });
	for (let i = 1; i<=WRITES; i++) {
		index.add(new Stable.Index.Entry(i,2,4,8));
	}
	index.close();

	let number;
	index.open();
	for (let entry of index.all()) {
		number = entry.number;
	}
	index.close();
	if (number !== WRITES) throw new Error('Not all entries were written! Last entry was '+number);
});

Suite.add('index [latest]', () => {
	const index = new Latest.Index('.index', { dataDirectory: 'data' });
	for (let i = 1; i<=WRITES; i++) {
		index.add(new Latest.Index.Entry(i,2,4,8));
	}
	index.close();

	let number;
	index.open();
	for (let entry of index.all()) {
		number = entry.number;
	}
	index.close();
	if (number !== WRITES) throw new Error('Not all entries were written! Last entry was '+number);
});

Suite.run();