import expect from 'expect.js';
import fs from 'fs-extra';
import { execFile } from 'child_process';
import { once } from 'events';
import { promisify } from 'util';
import { fileURLToPath } from 'url';
import AppendOnlyMmapedFile, { ReadOnly as ReadOnlyAppendOnlyMmapedFile, Readable as ReadableAppendOnlyMmapedFile } from '../src/AppendOnlyMmapedFile.js';

const execFileAsync = promisify(execFile);
const __dirname = fileURLToPath(new URL('.', import.meta.url));
const dataDirectory = __dirname + 'data';

describe('AppendOnlyMmapedFile', function () {

    beforeEach(function () {
        fs.emptyDirSync(dataDirectory);
    });

    it('writes and reads appended data', function () {
        const writer = new AppendOnlyMmapedFile('.mapped', {
            dataDirectory,
            pageSize: 64,
            writeBufferSize: 64,
            initialData: Buffer.from('HEAD')
        });
        writer.open();
        const position = writer.write('hello');
        writer.flush();
        writer.close();

        expect(position).to.be(4);

        const reader = new ReadableAppendOnlyMmapedFile('.mapped', {
            dataDirectory,
            pageSize: 64,
            writeBufferSize: 64
        });
        reader.open();
        expect(reader.readString(0, 4)).to.be('HEAD');
        expect(reader.readString(4, 5)).to.be('hello');
        reader.close();
    });

    it('remaps and emits append in readonly mode for a small cross-process flush', async function () {
        const writer = new AppendOnlyMmapedFile('.mapped', {
            dataDirectory,
            pageSize: 4096,
            writeBufferSize: 256,
            initialData: Buffer.from('HEAD')
        });
        writer.open();
        writer.flush();
        writer.close();

        const reader = new ReadOnlyAppendOnlyMmapedFile('.mapped', {
            dataDirectory,
            pageSize: 4096,
            writeBufferSize: 256
        });
        reader.open();

        const appendPromise = once(reader, 'append');
        const script = `
            import AppendOnlyMmapedFile from '${fileURLToPath(new URL('../src/AppendOnlyMmapedFile.js', import.meta.url)).replace(/\\/g, '\\\\')}';
            const writer = new AppendOnlyMmapedFile('.mapped', {
                dataDirectory: '${dataDirectory.replace(/\\/g, '\\\\')}',
                pageSize: 4096,
                writeBufferSize: 256
            });
            writer.open();
            writer.write('abc');
            writer.flush();
            writer.close();
        `;
        await execFileAsync(process.execPath, ['--input-type=module', '-e', script], { cwd: dataDirectory });

        const [previousSize, newSize] = await appendPromise;
        expect(newSize - previousSize).to.be(3);
        expect(reader.readString(previousSize, 3)).to.be('abc');
        reader.close();
    });

});
