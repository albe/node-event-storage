import Benchmark from 'benchmark';
import benchmarks from 'beautify-benchmark';
import fs from 'fs-extra';
import Stable from 'event-storage';
import { EventStore as Latest } from '../index.js';
import MmapWritableIndex, { MmapReadOnlyIndex, loadMmapModule } from '../src/Index/MmapWritableIndex.js';

const Suite = new Benchmark.Suite('eventstore');
Suite.on('start', () => fs.emptyDirSync('data'));
Suite.on('cycle', (event) => benchmarks.add(event.target));
Suite.on('complete', () => benchmarks.log());
Suite.on('error', (e) => console.log(e.target.error));

const WRITES = 1000;
const latestIndexOptions = {};

try {
	loadMmapModule();
	latestIndexOptions.IndexClass = MmapWritableIndex;
	latestIndexOptions.ReadOnlyIndexClass = MmapReadOnlyIndex;
} catch (e) {
	console.log('Mmap index unavailable, using latest default index implementation:', e.message);
}

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
	bench(new Latest('eventstore', {
		storageDirectory: 'data/latest',
		storageConfig: {
			indexOptions: latestIndexOptions
		}
	}), this.cycles);
});

Suite.run();
