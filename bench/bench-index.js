import Benchmark from 'benchmark';
import fs from 'fs-extra';
import Stable from 'event-storage';
import { Index as LatestIndex } from '../index.js';
import MmapWritableIndex, { getMmapPackageName } from '../src/Index/MmapWritableIndex.js';

const WRITE_DIR = 'data/write';
const READ_DIR = 'data/read';
const READ_SIZE = 1000;

function createEntry(number) {
    return new Stable.Index.Entry(number, 2, 4, 8);
}

function runSuite(name, register) {
    return new Promise((resolve, reject) => {
        const suite = new Benchmark.Suite(name);

        suite.on('start', () => {
            console.log(`\n${name}`);
        });
        suite.on('cycle', (event) => {
            console.log(String(event.target));
        });
        suite.on('error', (event) => {
            reject(event.target.error);
        });
        suite.on('complete', () => {
            resolve();
        });

        register(suite);
        suite.run({ async: false });
    });
}

function prepareWriteBenchmarks() {
    fs.emptyDirSync(WRITE_DIR);

    const indexes = [
        {
            label: 'stable',
            index: new Stable.Index('write.index', { dataDirectory: `${WRITE_DIR}/stable` }),
            count: 0,
        },
        {
            label: 'latest',
            index: new LatestIndex('write.index', { dataDirectory: `${WRITE_DIR}/latest` }),
            count: 0,
        },
    ];

    try {
        const packageName = getMmapPackageName();
        indexes.push({
            label: packageName,
            index: new MmapWritableIndex('write.index', { dataDirectory: `${WRITE_DIR}/mmap` }),
            count: 0,
        });
    } catch (e) {
        console.log('Skipping mmap write benchmark:', e.message);
    }

    return indexes;
}

function prepareReadBenchmarks() {
    fs.emptyDirSync(READ_DIR);

    const indexes = [
        {
            label: 'stable',
            index: new Stable.Index('read.index', { dataDirectory: `${READ_DIR}/stable` }),
        },
        {
            label: 'latest',
            index: new LatestIndex('read.index', { dataDirectory: `${READ_DIR}/latest` }),
        },
    ];

    try {
        const packageName = getMmapPackageName();
        indexes.push({
            label: packageName,
            index: new MmapWritableIndex('read.index', { dataDirectory: `${READ_DIR}/mmap` }),
        });
    } catch (e) {
        console.log('Skipping mmap read benchmark:', e.message);
    }

    for (const item of indexes) {
        item.index.open();
        for (let i = 1; i <= READ_SIZE; i++) {
            item.index.add(createEntry(i));
        }
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

async function main() {
    const writeIndexes = prepareWriteBenchmarks();
    const readIndexes = prepareReadBenchmarks();

    try {
        await runSuite('index write benchmark (open/close excluded)', (suite) => {
            for (const item of writeIndexes) {
                suite.add(`write:add [${item.label}]`, () => {
                    item.count += 1;
                    item.index.add(createEntry(item.count));
                });
            }
        });

        await runSuite('index read benchmark (open/close excluded)', (suite) => {
            for (const item of readIndexes) {
                suite.add(`read:range [${item.label}]`, () => {
                    let number = 0;
                    for (const entry of item.index.range(-READ_SIZE + 1)) {
                        number = entry.number;
                    }
                    if (number !== READ_SIZE) {
                        throw new Error(`Not all entries were read for ${item.label}. Last entry was ${number}`);
                    }
                });
            }
        });
    } finally {
        closeIndexes(writeIndexes);
        closeIndexes(readIndexes);
    }
}

main().catch((e) => {
    console.error(e);
    process.exitCode = 1;
});
