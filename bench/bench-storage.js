import Benchmark from 'benchmark';
import benchmarks from 'beautify-benchmark';
import fs from 'fs-extra';
import Stable from 'event-storage';
import { Storage as LatestStorage } from '../index.js';
import MmapWritableIndex from '../src/Index/MmapWritableIndex.js';
import MmapWritablePartition from '../src/Partition/MmapWritablePartition.js';

const Suite = new Benchmark.Suite('storage');
Suite.on('start', () => fs.emptyDirSync('data'));
Suite.on('cycle', (event) => benchmarks.add(event.target));
Suite.on('complete', () => benchmarks.log());
Suite.on('error', (e) => console.log(e.target.error));

const WRITES = 1000;
const READER_STEPS = [1, 2, 4, 8, 16];

class MmapStorage extends LatestStorage {
	createIndex(name, options = {}) {
		return { index: new MmapWritableIndex(name, options) };
	}

	createPartition(name, options = {}) {
		return new MmapWritablePartition(name, options);
	}
}

function bench(storage, readers) {
	storage.open();
	const doc = { doc: 'this is some test document for measuring performance', value: 123.45, amount: 999, pad: ' '.repeat(32), seq: 0 };
	for (let i = 1; i<=WRITES; i++) {
		doc.seq = i;
		storage.write(doc);
	}
	storage.close();

	storage.open();
	for (let reader = 0; reader < readers; reader++) {
		const start = Math.floor((reader * WRITES) / readers) + 1;
		const end = Math.floor(((reader + 1) * WRITES) / readers);
		const expectedLength = end - start + 1;
		let number = 0;
		let seq = 0;
		for (let readDoc of storage.readRange(start, end)) {
			number++;
			seq = readDoc.seq;
		}
		if (number !== expectedLength || seq !== end) {
			throw new Error(
				'Split read range failed for reader ' + reader +
				': expected [' + start + ',' + end + '] but got last=' + seq +
				' length=' + number
			);
		}
	}
	storage.close();
}

function addBenchmarks(label, createStorage) {
	let callCount = 0;
	for (const readers of READER_STEPS) {
		Suite.add(`storage [${label}] readers=${readers}`, function() {
			bench(createStorage(callCount++), readers);
		});
	}
}

addBenchmarks('stable', (id) => new Stable.Storage('storage-' + id, { dataDirectory: 'data/stable' }));
addBenchmarks('latest', (id) => new LatestStorage('storage-' + id, { dataDirectory: 'data/latest' }));
addBenchmarks('mmap', (id) => new MmapStorage('storage-' + id, { dataDirectory: 'data/mmap' }));

Suite.run();
