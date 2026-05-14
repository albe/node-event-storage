import fs from 'fs-extra';
import Stable from 'event-storage';
import { Index as LatestIndex } from '../index.js';
import MmapWritableIndex, { getMmapPackageName } from '../src/Index/MmapWritableIndex.js';

const WRITE_DIR = 'data/write';
const READ_DIR = 'data/read';
const READ_SIZE = 1000;
const WRITE_WARMUP = 5000;
const WRITE_ITERATIONS = 50000;
const READ_WARMUP = 200;
const READ_ITERATIONS = 2000;

function createEntry(number) {
    return new Stable.Index.Entry(number, 2, 4, 8);
}

function median(values) {
    const sorted = values.slice().sort((a, b) => a - b);
    return sorted[Math.floor(sorted.length / 2)];
}

function measure(label, iterations, warmup, fn) {
    for (let i = 0; i < warmup; i++) fn();

    const samples = [];
    const chunkSize = Math.max(1, Math.floor(iterations / 20));
    let done = 0;

    while (done < iterations) {
        const count = Math.min(chunkSize, iterations - done);
        const start = process.hrtime.bigint();
        for (let i = 0; i < count; i++) fn();
        const elapsedNs = Number(process.hrtime.bigint() - start);
        samples.push(elapsedNs / count);
        done += count;
    }

    const nsPerOp = median(samples);
    const opsPerSec = 1e9 / nsPerOp;
    console.log(`${label.padEnd(32)} ${opsPerSec.toFixed(0).padStart(10)} ops/sec  ${nsPerOp.toFixed(0).padStart(10)} ns/op`);

    return { nsPerOp, opsPerSec };
}

function createIndexes(baseDir, fileName) {
    const indexes = [
        {
            label: 'stable',
            index: new Stable.Index(fileName, { dataDirectory: `${baseDir}/stable` }),
        },
        {
            label: 'latest',
            index: new LatestIndex(fileName, { dataDirectory: `${baseDir}/latest` }),
        },
    ];

    try {
        const packageName = getMmapPackageName();
        indexes.push({
            label: packageName,
            index: new MmapWritableIndex(fileName, { dataDirectory: `${baseDir}/mmap` }),
        });
    } catch (e) {
        console.log('Skipping mmap benchmark:', e.message);
    }

    for (const item of indexes) {
        item.index.open();
    }

    return indexes;
}

function closeIndexes(indexes) {
    for (const item of indexes) {
        try {
            item.index.close();
        } catch (e) {
        }
    }
}

function runWriteBenchmarks() {
    fs.emptyDirSync(WRITE_DIR);
    const indexes = createIndexes(WRITE_DIR, 'write.index');

    console.log('\nindex write benchmark (add only, open/close excluded)');
    const results = {};

    try {
        for (const item of indexes) {
            let count = 0;
            results[item.label] = measure(
                `write:add [${item.label}]`,
                WRITE_ITERATIONS,
                WRITE_WARMUP,
                () => {
                    count += 1;
                    item.index.add(createEntry(count));
                }
            );
        }
    } finally {
        closeIndexes(indexes);
    }

    return results;
}

function runReadBenchmarks() {
    fs.emptyDirSync(READ_DIR);
    const indexes = createIndexes(READ_DIR, 'read.index');

    for (const item of indexes) {
        for (let i = 1; i <= READ_SIZE; i++) {
            item.index.add(createEntry(i));
        }
    }

    console.log('\nindex read benchmark (range iteration only, open/close excluded)');
    const results = {};

    try {
        for (const item of indexes) {
            results[item.label] = measure(
                `read:range [${item.label}]`,
                READ_ITERATIONS,
                READ_WARMUP,
                () => {
                    let number = 0;
                    for (const entry of item.index.range(-READ_SIZE + 1)) {
                        number = entry.number;
                    }
                    if (number !== READ_SIZE) {
                        throw new Error(`Read validation failed for ${item.label}: ${number}`);
                    }
                }
            );
        }
    } finally {
        closeIndexes(indexes);
    }

    return results;
}

function printRelativeComparison(title, results) {
    const stable = results.stable?.nsPerOp;
    if (!stable) {
        return;
    }

    console.log(`\n${title} (relative to stable)`);
    for (const [label, result] of Object.entries(results)) {
        const delta = ((result.nsPerOp / stable - 1) * 100).toFixed(1);
        const sign = delta.startsWith('-') ? '' : '+';
        console.log(`  ${label.padEnd(20)} ${sign}${delta}%`);
    }
}

function main() {
    const writeResults = runWriteBenchmarks();
    const readResults = runReadBenchmarks();

    printRelativeComparison('write performance', writeResults);
    printRelativeComparison('read performance', readResults);
}

main();
