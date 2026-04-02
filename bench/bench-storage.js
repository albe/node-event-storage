import Benchmark from 'benchmark';
import benchmarks from 'beautify-benchmark';
import fs from 'fs-extra';
import Stable from 'event-storage';
import { Storage as LatestStorage } from '../index.js';

const Suite = new Benchmark.Suite('storage');
Suite.on('start', () => fs.emptyDirSync('data'));
Suite.on('cycle', (event) => benchmarks.add(event.target));
Suite.on('complete', () => benchmarks.log());
Suite.on('error', (e) => console.log(e.target.error));

const WRITES = 1000;

function bench(storage) {
	storage.open();
	let firstPosition = Number.MAX_SAFE_INTEGER;
	const doc = { doc: 'this is some test document for measuring performance', value: 123.45, amount: 999, pad: ' '.repeat(32) };
	for (let i = 1; i<=WRITES; i++) {
		firstPosition = Math.min(firstPosition, storage.write(doc));
	}
	storage.close();

	let number = 0;
	storage.open();
	for (let doc of storage.readRange(firstPosition)) {
		number++;
	}
	storage.close();
	if (number < WRITES) throw new Error('Not all documents were written! Last document was '+number);
}

Suite.add('storage [stable]', function() {
	bench(new Stable.Storage('storage-' + this.cycles, { dataDirectory: 'data/stable' }));
});

Suite.add('storage [latest]', function() {
	bench(new LatestStorage('storage-' + this.cycles, { dataDirectory: 'data/latest' }));
});

Suite.run();