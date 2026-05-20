import expect from 'expect.js';
import fs from 'fs-extra';
import path from 'path';
import { iterate } from '../src/util.js';
import { scanForFiles } from '../src/fsUtil.js';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const testDir = path.join(__dirname, 'data', 'util-scan-test');

describe('util', function() {

    describe('iterate', function() {

        it('iterates entries forwards when requested', function() {
            const entries = ['a', 'b', 'c'];

            expect(Array.from(iterate(entries, true))).to.eql(['a', 'b', 'c']);
            expect(Array.from(iterate(entries, false))).to.eql(['c', 'b', 'a']);
        });

        it('can be used directly in a for-of loop', function() {
            const entries = ['x', 'y'];
            const seen = [];

            for (const entry of iterate(entries, false)) {
                seen.push(entry);
            }

            expect(seen).to.eql(['y', 'x']);
        });

    });

    describe('scanForFiles', function() {

        beforeEach(function() {
            fs.emptyDirSync(testDir);
        });

        afterEach(function() {
            fs.removeSync(testDir);
        });

        it('calls onEach for each matching file and onDone with no error', function(done) {
            fs.writeFileSync(path.join(testDir, 'stream-foo.index'), '');
            fs.writeFileSync(path.join(testDir, 'stream-bar.index'), '');
            fs.writeFileSync(path.join(testDir, 'unrelated.txt'), '');

            const found = [];
            scanForFiles(testDir, /(stream-.*)\.index$/, (name) => {
                found.push(name);
            }, (err) => {
                expect(err).to.be(null);
                expect(found.sort()).to.eql(['stream-bar', 'stream-foo']);
                done();
            });
        });

        it('calls onDone with no matches when no files match', function(done) {
            fs.writeFileSync(path.join(testDir, 'unrelated.txt'), '');

            const found = [];
            scanForFiles(testDir, /(stream-.*)\.index$/, (name) => {
                found.push(name);
            }, (err) => {
                expect(err).to.be(null);
                expect(found).to.eql([]);
                done();
            });
        });

        it('falls back to the full match when there is no capturing group', function(done) {
            fs.writeFileSync(path.join(testDir, 'stream-foo.index'), '');

            const found = [];
            scanForFiles(testDir, /stream-.*\.index$/, (name) => {
                found.push(name);
            }, (err) => {
                expect(err).to.be(null);
                expect(found).to.eql(['stream-foo.index']);
                done();
            });
        });

        it('calls onDone with an error when the directory does not exist', function(done) {
            scanForFiles(path.join(testDir, 'nonexistent'), /.*/, () => {}, (err) => {
                expect(err).to.be.an(Error);
                done();
            });
        });

    });

});
