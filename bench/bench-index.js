import Benchmark from 'benchmark';
import benchmarks from 'beautify-benchmark';
import fs from 'fs-extra';
import Stable from 'event-storage';
import { createRequire } from 'module';
import path from 'path';
import { Index as LatestIndex } from '../index.js';

const require = createRequire(import.meta.url);
const ENTRY_SIZE = 16;

let mmapModule = null;
let mmapPackageName = null;

function resolveMmapModule() {
	if (mmapModule) {
		return mmapModule;
	}
	const packageNames = ['@riaskov/mmap-io', 'mmap-io', '@fayzanx/mmap-io'];
	let lastError = null;
	for (const packageName of packageNames) {
		try {
			mmapModule = require(packageName);
			mmapPackageName = packageName;
			return mmapModule;
		} catch (e) {
			lastError = e;
		}
	}
	if (lastError) {
		throw new Error(`No compatible mmap-io implementation installed: ${lastError.message}`);
	}
	throw new Error('No compatible mmap-io implementation installed.');
}

class MmapIndex {
	constructor(name = '.index', options = {}) {
		resolveMmapModule();
		this.name = name;
		this.dataDirectory = options.dataDirectory || '.';
		this.mappedEntries = options.mappedEntries || 2048;
		this.fileName = path.resolve(this.dataDirectory, this.name);
		this.fd = null;
		this.length = 0;
		this.mapBuffer = null;
		this.open();
	}

	open() {
		if (this.fd) {
			return false;
		}
		fs.ensureDirSync(this.dataDirectory);
		this.fd = fs.openSync(this.fileName, 'a+');
		this.loadLength();
		this.mapEntries();
		return true;
	}

	close() {
		if (this.fd) {
			if (this.mapBuffer) {
				resolveMmapModule().sync(this.mapBuffer);
			}
			fs.ftruncateSync(this.fd, this.length * ENTRY_SIZE);
			fs.closeSync(this.fd);
			this.fd = null;
		}
		this.unmapEntries();
	}

	add(entry) {
		const nextLength = this.length + 1;
		if (nextLength > this.mappedEntries) {
			throw new Error(`Mapped index capacity exceeded: ${nextLength} > ${this.mappedEntries}`);
		}
		const offset = (nextLength - 1) * ENTRY_SIZE;
		this.mapBuffer.writeUInt32LE(entry.number ?? entry[0], offset);
		this.mapBuffer.writeUInt32LE(entry.position ?? entry[1], offset + 4);
		this.mapBuffer.writeUInt32LE(entry.size ?? entry[2] ?? 0, offset + 8);
		this.mapBuffer.writeUInt32LE(entry.partition ?? entry[3] ?? 0, offset + 12);
		this.length = nextLength;
		return nextLength;
	}

	range(from, until = -1) {
		from = this.wrapAndCheck(from);
		until = this.wrapAndCheck(until);
		if (from <= 0 || until < from) {
			return false;
		}
		const entries = new Array(until - from + 1);
		for (let i = 0; i < entries.length; i++) {
			entries[i] = this.readEntry(from + i);
		}
		return entries;
	}

	loadLength() {
		const stat = fs.fstatSync(this.fd);
		if (stat.size % ENTRY_SIZE !== 0) {
			throw new Error(`Corrupted mmap index file: ${this.fileName}`);
		}
		this.length = stat.size / ENTRY_SIZE;
	}

	mapEntries() {
		const mmap = resolveMmapModule();
		this.unmapEntries();
		const mapLength = Math.max(this.length, this.mappedEntries);
		fs.ftruncateSync(this.fd, mapLength * ENTRY_SIZE);
		this.mapBuffer = mmap.map(mapLength * ENTRY_SIZE, mmap.PROT_READ | mmap.PROT_WRITE, mmap.MAP_SHARED, this.fd, 0);
	}

	unmapEntries() {
		if (!this.mapBuffer) {
			return;
		}
		this.mapBuffer = null;
	}

	readEntry(index) {
		if (!this.mapBuffer) {
			return false;
		}
		const offset = (index - 1) * ENTRY_SIZE;
		return {
			number: this.mapBuffer.readUInt32LE(offset),
			position: this.mapBuffer.readUInt32LE(offset + 4),
			size: this.mapBuffer.readUInt32LE(offset + 8),
			partition: this.mapBuffer.readUInt32LE(offset + 12)
		};
	}

	wrapAndCheck(index) {
		index = Number(index);
		if (!Number.isFinite(index)) {
			return 0;
		}
		if (index < 0) {
			index = this.length + 1 + index;
		}
		return index > this.length ? 0 : index;
	}
}

const Suite = new Benchmark.Suite('index');
Suite.on('start', () => fs.emptyDirSync('data'));
Suite.on('cycle', (event) => benchmarks.add(event.target));
Suite.on('complete', () => benchmarks.log());
Suite.on('error', (e) => console.log(e.target.error));

const WRITES = 1000;
let stableCallCount = 0;
let latestCallCount = 0;
let mmapCallCount = 0;

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
	bench(new Stable.Index((stableCallCount++) + '.index', { dataDirectory: 'data/stable' }));
});

Suite.add('index [latest]', function() {
	bench(new LatestIndex((latestCallCount++) + '.index', { dataDirectory: 'data/latest' }));
});

try {
	const packageName = resolveMmapModule() && mmapPackageName;
	Suite.add(`index [${packageName}]`, function() {
		bench(new MmapIndex((mmapCallCount++) + '.index', { dataDirectory: 'data/mmap' }));
	});
} catch (e) {
	console.log('Skipping mmap benchmark:', e.message);
}

Suite.run();
