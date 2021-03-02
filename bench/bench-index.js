const Benchmark = require('benchmark');
const benchmarks = require('beautify-benchmark');
const fs = require('fs-extra');

const Suite = new Benchmark.Suite('index');
Suite.on('start', () => fs.emptyDirSync('data'));
Suite.on('cycle', (event) => benchmarks.add(event.target));
Suite.on('complete', () => benchmarks.log());
Suite.on('error', (e) => console.log(e.target.error));

const Stable = require('event-storage');
const Latest = require('../index');

const WRITES = 1000;

function bench(index) {
	index.open();
	for (let i = 1; i<=WRITES; i++) {
		index.add(new Stable.Index.Entry(i,2,4,8));
	}
	index.close();

	let number;
	index.open();
	for (let entry of index.range(-WRITES + 1)) {
		number = entry.number;
	}
	index.close();
	if (number < WRITES) throw new Error('Not all entries were written! Last entry was '+number);
}

Suite.add('index [stable]', function() {
	bench(new Stable.Index(this.cycles + '.index', { dataDirectory: 'data/stable' }));
});

Suite.add('index [latest]', function() {
	bench(new Latest.Index(this.cycles + '.index', { dataDirectory: 'data/latest' }));
});

Suite.run();