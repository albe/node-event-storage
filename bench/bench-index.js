import Benchmark from 'benchmark';
import benchmarks from 'beautify-benchmark';
import fs from 'fs-extra';
import Stable from 'event-storage';
import { Index as LatestIndex, MmapIndex } from '../index.js';

const Suite = new Benchmark.Suite('index');
Suite.on('start', () => fs.emptyDirSync('data'));
Suite.on('cycle', (event) => benchmarks.add(event.target));
Suite.on('complete', () => benchmarks.log());
Suite.on('error', (e) => console.log(e.target.error));

const WRITES = 1000;
const READER_STEPS = [1, 2, 4, 8, 16];

function bench({ IndexClass, dataDirectory, id, readers }) {
	const name = `${id}.index`;
	const writer = new IndexClass(name, { dataDirectory });
	const readIndexes = [];
	const ReaderClass = IndexClass.ReadOnly || IndexClass;

	for (let reader = 0; reader < readers; reader++) {
		const readIndex = new ReaderClass(name, { dataDirectory });
		if (typeof readIndex.range !== 'function') {
			readIndexes.push(new IndexClass(name, { dataDirectory }));
			continue;
		}
		readIndexes.push(readIndex);
	}

	writer.open();
	for (const index of readIndexes) {
		index.open();
	}

	for (let i = 1; i <= WRITES; i++) {
		writer.add(new IndexClass.Entry(i, 2, 4, 8));
	}
	writer.flush();

	for (const index of readIndexes) {
		index.close();
		index.open();
		let number = 0;
		const entries = index.range(-WRITES + 1) || [];
		for (const entry of entries) {
			number = entry.number;
		}
		if (number < WRITES) {
			throw new Error('Not all entries were written! Last entry was ' + number);
		}
	}

	for (const index of readIndexes) {
		index.close();
	}
	writer.close();
}

function addBenchmarks(label, IndexClass, dataDirectory) {
	let callCount = 0;
	for (const readers of READER_STEPS) {
		Suite.add(`index [${label}] readers=${readers}`, function() {
			bench({
				IndexClass,
				dataDirectory,
				id: callCount++,
				readers
			});
		});
	}
}

addBenchmarks('stable', Stable.Index, 'data/stable');
addBenchmarks('latest', LatestIndex, 'data/latest');
addBenchmarks('mmap', MmapIndex, 'data/mmap');

Suite.run();
