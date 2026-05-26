import expect from 'expect.js';
import os from 'os';
import path from 'path';
import fs from 'fs-extra';
import MmapPartition, { ReadOnly as MmapReadOnlyPartition } from '../src/MmapPartition.js';

describe('MmapPartition', function () {
    const dataDirectory = path.join(os.tmpdir(), 'event-store-mmap-partition-tests');

    beforeEach(function () {
        fs.emptyDirSync(dataDirectory);
    });

    afterEach(function () {
        fs.removeSync(dataDirectory);
    });

    it('writes and reads documents', function () {
        const partition = new MmapPartition('part-1', { dataDirectory, flushDelay: 0 });
        partition.open();

        const first = partition.write('one', 0);
        const second = partition.write('two', 1);
        partition.flush();

        expect(partition.readFrom(first)).to.be('one');
        expect(partition.readFrom(second)).to.be('two');

        partition.close();
        partition.open();

        expect(partition.readFrom(first)).to.be('one');
        expect(partition.readFrom(second)).to.be('two');

        partition.close();
    });

    it('emits append in readonly mode after writer flushes', function (done) {
        this.timeout(10000);

        const writer = new MmapPartition('part-2', { dataDirectory, flushDelay: 0 });
        writer.open();
        writer.write('initial', 0);
        writer.flush();

        const reader = new MmapReadOnlyPartition('part-2', { dataDirectory });
        reader.open();

        reader.once('append', function (prevSize, nextSize) {
            expect(nextSize).to.be.greaterThan(prevSize);
            expect(reader.readFrom(prevSize)).to.be('next');
            reader.close();
            writer.close();
            done();
        });

        writer.write('next', 1);
        writer.flush();
    });
});
