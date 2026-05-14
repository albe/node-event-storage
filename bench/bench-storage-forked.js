import fs from 'fs-extra';
import { fork } from 'child_process';
import { createRequire } from 'module';
import { fileURLToPath } from 'url';
import { hrtime } from 'process';
import Stable from 'event-storage';
import { Storage as LatestStorage } from '../index.js';
import LatestReadableStorage from '../src/Storage/ReadableStorage.js';
import MmapWritableIndex from '../src/Index/MmapWritableIndex.js';
import MmapReadOnlyIndex from '../src/Index/MmapReadOnlyIndex.js';
import MmapWritablePartition from '../src/Partition/MmapWritablePartition.js';
import MmapReadOnlyPartition from '../src/Partition/MmapReadOnlyPartition.js';

const require = createRequire(import.meta.url);
const StableReadableStorage = require('event-storage/src/Storage/ReadableStorage');
const WRITES = 1000;
const READER_STEPS = [1, 2, 4, 8, 16];
const scriptPath = fileURLToPath(import.meta.url);
const readerWarmupDelayNs = 50_000_000n;

class MmapStorage extends LatestStorage {
	createIndex(name, options = {}) {
		return { index: new MmapWritableIndex(name, options) };
	}

	createPartition(name, options = {}) {
		return new MmapWritablePartition(name, options);
	}
}

class MmapReadableStorage extends LatestReadableStorage {
	createIndex(name, options = {}) {
		return { index: new MmapReadOnlyIndex(name, options) };
	}

	createPartition(name, options = {}) {
		return new MmapReadOnlyPartition(name, options);
	}
}

function getStableWriterClass() {
	return Stable.Storage;
}

function getStableReaderClass() {
	return StableReadableStorage;
}

function getVariantConfig(variant) {
	if (variant === 'stable') {
		return {
			label: 'stable',
			dataDirectory: 'data/stable-forked',
			WriterClass: getStableWriterClass(),
			ReaderClass: getStableReaderClass()
		};
	}
	if (variant === 'latest') {
		return {
			label: 'latest',
			dataDirectory: 'data/latest-forked',
			WriterClass: LatestStorage,
			ReaderClass: LatestReadableStorage
		};
	}
	if (variant === 'mmap') {
		return {
			label: 'mmap',
			dataDirectory: 'data/mmap-forked',
			WriterClass: MmapStorage,
			ReaderClass: MmapReadableStorage
		};
	}
	throw new Error(`Unknown variant "${variant}".`);
}

function createWriterStorage(variant, storageName, dataDirectory) {
	const { WriterClass } = getVariantConfig(variant);
	return new WriterClass(storageName, { dataDirectory, syncOnFlush: true });
}

function createReaderStorage(variant, storageName, dataDirectory) {
	const { ReaderClass } = getVariantConfig(variant);
	if (!ReaderClass) {
		throw new Error(`Variant "${variant}" does not expose a read-only storage.`);
	}
	return new ReaderClass(storageName, { dataDirectory });
}

function openStorage(storage) {
	return new Promise((resolve, reject) => {
		let settled = false;
		let timeout = null;
		const finish = () => {
			if (settled) {
				return;
			}
			settled = true;
			if (timeout) {
				clearImmediate(timeout);
			}
			storage.removeListener?.('opened', finish);
			storage.removeListener?.('error', fail);
			resolve(storage);
		};
		const fail = (error) => {
			if (settled) {
				return;
			}
			settled = true;
			if (timeout) {
				clearImmediate(timeout);
			}
			storage.removeListener?.('opened', finish);
			storage.removeListener?.('error', fail);
			reject(error);
		};

		storage.once?.('opened', finish);
		storage.once?.('error', fail);
		storage.open(finish);
		timeout = setImmediate(() => {
			const initialized = !('initialized' in storage) || storage.initialized !== false;
			const indexOpen = typeof storage.index?.isOpen !== 'function' || storage.index.isOpen();
			if (initialized && indexOpen) {
				finish();
			}
		});
	});
}

function toMilliseconds(durationNs) {
	return Number(durationNs) / 1e6;
}

function formatMilliseconds(durationNs) {
	return toMilliseconds(durationNs).toFixed(3);
}

function percentile(sortedValues, ratio) {
	if (sortedValues.length === 0) {
		return 0n;
	}
	const index = Math.min(sortedValues.length - 1, Math.ceil(sortedValues.length * ratio) - 1);
	return sortedValues[index];
}

function serializeError(error) {
	return {
		message: error?.message || String(error),
		stack: error?.stack || ''
	};
}

function createReaderProcess({ variant, storageName, dataDirectory, writes, readerId }) {
	const child = fork(scriptPath, ['worker'], {
		stdio: ['inherit', 'inherit', 'inherit', 'ipc'],
		env: {
			...process.env,
			BENCH_VARIANT: variant,
			BENCH_STORAGE_NAME: storageName,
			BENCH_DATA_DIRECTORY: dataDirectory,
			BENCH_WRITES: String(writes),
			BENCH_READER_ID: String(readerId)
		}
	});
	const messageQueue = new Map();
	const waiters = new Map();
	let settledDone = false;
	let resolveDone;
	let rejectDone;
	let resolveExit;
	let rejectExit;

	const donePromise = new Promise((resolve, reject) => {
		resolveDone = resolve;
		rejectDone = reject;
	});
	const exitPromise = new Promise((resolve, reject) => {
		resolveExit = resolve;
		rejectExit = reject;
	});

	child.on('message', (message) => {
		if (!message || typeof message.type !== 'string') {
			return;
		}
		if (message.type === 'error') {
			const error = new Error(message.error?.message || 'Child process failed.');
			if (message.error?.stack) {
				error.stack = message.error.stack;
			}
			rejectDone(error);
			const waiter = waiters.get('error');
			if (waiter) {
				waiters.delete('error');
				waiter.reject(error);
			}
			return;
		}
		if (message.type === 'done') {
			settledDone = true;
			resolveDone(message);
		}
		const waiter = waiters.get(message.type);
		if (waiter) {
			waiters.delete(message.type);
			waiter.resolve(message);
			return;
		}
		const queuedMessages = messageQueue.get(message.type) || [];
		queuedMessages.push(message);
		messageQueue.set(message.type, queuedMessages);
	});

	child.once('exit', (code, signal) => {
		resolveExit();
		if (!settledDone && code !== 0) {
			rejectDone(new Error(`Reader process ${readerId} exited with code ${code} and signal ${signal}.`));
		}
	});

	child.once('error', (error) => {
		rejectDone(error);
		rejectExit(error);
	});

	return {
		child,
		donePromise,
		exitPromise,
		send(message) {
			child.send(message);
		},
		waitFor(type) {
			return new Promise((resolve, reject) => {
				const queuedMessages = messageQueue.get(type);
				if (queuedMessages?.length) {
					const message = queuedMessages.shift();
					if (queuedMessages.length === 0) {
						messageQueue.delete(type);
					}
					resolve(message);
					return;
				}
				waiters.set(type, { resolve, reject });
			});
		}
	};
}

async function runCase(variant, readers) {
	const { label, dataDirectory } = getVariantConfig(variant);
	const storageName = `storage-forked-${label}-${readers}`;
	fs.emptyDirSync(dataDirectory);

	const writer = createWriterStorage(variant, storageName, dataDirectory);
	await openStorage(writer);

	const readerProcesses = Array.from({ length: readers }, (_, readerId) => createReaderProcess({
		variant,
		storageName,
		dataDirectory,
		writes: WRITES,
		readerId
	}));

	try {
		await Promise.all(readerProcesses.map(readerProcess => readerProcess.waitFor('ready')));
		readerProcesses.forEach(readerProcess => readerProcess.send({ type: 'arm' }));
		await Promise.all(readerProcesses.map(readerProcess => readerProcess.waitFor('armed')));

		const startNs = hrtime.bigint() + readerWarmupDelayNs;
		readerProcesses.forEach(readerProcess => readerProcess.send({ type: 'start', startNs: startNs.toString() }));

		const waitMs = Math.max(0, Math.ceil(Number(startNs - hrtime.bigint()) / 1e6));
		if (waitMs > 0) {
			await new Promise(resolve => setTimeout(resolve, waitMs));
		}

		const baseDocument = {
			doc: 'this is some test document for measuring performance',
			value: 123.45,
			amount: 999,
			pad: ' '.repeat(32),
			seq: 0,
			writeStartedAtNs: '0'
		};

		for (let number = 1; number <= WRITES; number++) {
			baseDocument.seq = number;
			baseDocument.writeStartedAtNs = hrtime.bigint().toString();
			writer.write(baseDocument);
			writer.flush();
		}

		const results = await Promise.all(readerProcesses.map(readerProcess => readerProcess.donePromise));
		const durationsNs = results.map(result => BigInt(result.durationNs));
		const totalDurationNs = durationsNs.reduce((max, durationNs) => durationNs > max ? durationNs : max, 0n);
		const averageDurationNs = durationsNs.reduce((sum, durationNs) => sum + durationNs, 0n) / BigInt(durationsNs.length);
		const latencyValues = results
			.flatMap(result => result.latenciesNs.map(value => BigInt(value)))
			.sort((left, right) => left < right ? -1 : left > right ? 1 : 0);
		const totalLatencyNs = latencyValues.reduce((sum, value) => sum + value, 0n);
		const averageLatencyNs = totalLatencyNs / BigInt(latencyValues.length);
		const p95LatencyNs = percentile(latencyValues, 0.95);
		const maxLatencyNs = latencyValues[latencyValues.length - 1];

		return {
			variant: label,
			readers,
			totalDurationNs,
			averageDurationNs,
			averageLatencyNs,
			p95LatencyNs,
			maxLatencyNs
		};
	} finally {
		writer.close();
		await Promise.all(readerProcesses.map(async (readerProcess) => {
			readerProcess.send({ type: 'stop' });
			await readerProcess.exitPromise;
		}));
	}
}

function printResults(rows) {
	const headers = ['Variant', 'Readers', 'All readers done (ms)', 'Avg reader done (ms)', 'Avg visibility latency (ms)', 'P95 latency (ms)', 'Max latency (ms)'];
	const table = rows.map((row) => ([
		row.variant,
		String(row.readers),
		formatMilliseconds(row.totalDurationNs),
		formatMilliseconds(row.averageDurationNs),
		formatMilliseconds(row.averageLatencyNs),
		formatMilliseconds(row.p95LatencyNs),
		formatMilliseconds(row.maxLatencyNs)
	]));
	const widths = headers.map((header, index) => Math.max(header.length, ...table.map(row => row[index].length)));
	const formatRow = (row) => `| ${row.map((value, index) => value.padEnd(widths[index])).join(' | ')} |`;
	const separator = `|-${widths.map(width => '-'.repeat(width)).join('-|-')}-|`;

	console.log(formatRow(headers));
	console.log(separator);
	for (const row of table) {
		console.log(formatRow(row));
	}
}

async function runParent() {
	const rows = [];
	for (const variant of ['stable', 'latest', 'mmap']) {
		for (const readers of READER_STEPS) {
			rows.push(await runCase(variant, readers));
		}
	}
	printResults(rows);
}

async function runWorker() {
	const variant = process.env.BENCH_VARIANT;
	const storageName = process.env.BENCH_STORAGE_NAME;
	const dataDirectory = process.env.BENCH_DATA_DIRECTORY;
	const writes = Number(process.env.BENCH_WRITES);
	const readerId = Number(process.env.BENCH_READER_ID);
	const storage = createReaderStorage(variant, storageName, dataDirectory);
	let active = false;
	let draining = false;
	let benchmarkStartNs = 0n;
	let seen = 0;
	const latenciesNs = [];
	let pollTimer = null;

	const stopPolling = () => {
		if (pollTimer) {
			clearInterval(pollTimer);
			pollTimer = null;
		}
	};

	const refreshReadableLength = () => {
		if (typeof storage.index?.readFileLength !== 'function' || !Array.isArray(storage.index?.data)) {
			return;
		}
		const refreshedLength = storage.index.readFileLength();
		if (refreshedLength > storage.index.data.length) {
			storage.index.data.length = refreshedLength;
		}
	};

	const refreshPartitions = () => {
		const files = fs.readdirSync(storage.dataDirectory);
		for (const file of files) {
			if (!file.startsWith(storage.storageFile) || file.endsWith('.index') || file.endsWith('.branch') || file.endsWith('.lock')) {
				continue;
			}
			const partition = storage.createPartition(file, storage.partitionConfig);
			if (typeof storage.partitions?.has === 'function' && typeof storage.partitions?.add === 'function') {
				if (!storage.partitions.has(partition.id)) {
					storage.partitions.add(partition.id, partition);
				}
				continue;
			}
			if (!(partition.id in storage.partitions)) {
				storage.partitions[partition.id] = partition;
			}
		}
	};

	async function drainDocuments() {
		if (draining) {
			return;
		}
		draining = true;
		try {
			while (active && seen < writes) {
				const document = storage.read(seen + 1);
				if (document === false) {
					return;
				}
				if (document.seq !== seen + 1) {
					throw new Error(`Reader ${readerId} expected document #${seen + 1} but received #${document.seq}.`);
				}
				const latencyNs = hrtime.bigint() - BigInt(document.writeStartedAtNs);
				latenciesNs.push(latencyNs.toString());
				seen++;
			}

			if (active && seen === writes) {
				active = false;
				stopPolling();
				process.send?.({
					type: 'done',
					readerId,
					durationNs: (hrtime.bigint() - benchmarkStartNs).toString(),
					latenciesNs
				});
			}
		} finally {
			draining = false;
		}
	}

	process.on('message', async (message) => {
		try {
			if (message.type === 'arm') {
				process.send?.({ type: 'armed', readerId });
				return;
			}
			if (message.type === 'start') {
				benchmarkStartNs = BigInt(message.startNs);
				active = true;
				if (!pollTimer) {
					pollTimer = setInterval(() => {
						try {
							refreshPartitions();
							refreshReadableLength();
							drainDocuments().catch((error) => {
								process.send?.({ type: 'error', error: serializeError(error), readerId });
							});
						} catch (error) {
							process.send?.({ type: 'error', error: serializeError(error), readerId });
						}
					}, 1);
					pollTimer.unref?.();
				}
				await drainDocuments();
				return;
			}
			if (message.type === 'stop') {
				stopPolling();
				storage.close();
				process.exit(0);
			}
		} catch (error) {
			process.send?.({ type: 'error', error: serializeError(error), readerId });
		}
	});

	try {
		await openStorage(storage);
		process.send?.({ type: 'ready', readerId });
	} catch (error) {
		process.send?.({ type: 'error', error: serializeError(error), readerId });
	}
}

if (process.argv[2] === 'worker') {
	runWorker();
} else {
	runParent().catch((error) => {
		console.error(error);
		process.exit(1);
	});
}
