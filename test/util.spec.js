import expect from 'expect.js';
import fs from 'fs-extra';
import path from 'path';
import { iterate, compileAccessor } from '../src/utils/util.js';
import { safeUnlink, scanForFiles, writeFileAtomic } from '../src/utils/fsUtil.js';
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

    describe('writeFileAtomic', function() {

        beforeEach(function() {
            fs.emptyDirSync(testDir);
        });

        after(function() {
            fs.removeSync(testDir);
        });

        it('replaces the target file and removes the temporary file', function() {
            const target = path.join(testDir, 'state');
            fs.writeFileSync(target, 'old');
            writeFileAtomic(target, 'new', target + '.1');
            expect(fs.readFileSync(target, 'utf8')).to.be('new');
            expect(fs.existsSync(target + '.1')).to.be(false);
        });

        it('keeps the previous target and cleans up when the rename fails', function() {
            const target = path.join(testDir, 'state');
            fs.writeFileSync(target, 'old');
            fs.mkdirSync(path.join(testDir, 'blocker'));
            fs.writeFileSync(path.join(testDir, 'blocker', 'file'), '');
            const tmp = path.join(testDir, 'state.tmp');
            // Renaming a file over a non-empty directory fails.
            expect(() => writeFileAtomic(path.join(testDir, 'blocker'), 'new', tmp)).to.throwError();
            expect(fs.existsSync(tmp)).to.be(false);
            expect(fs.readFileSync(target, 'utf8')).to.be('old');
        });

        it('defaults the temporary file name to target + .tmp', function() {
            const target = path.join(testDir, 'state');
            writeFileAtomic(target, 'data');
            expect(fs.readFileSync(target, 'utf8')).to.be('data');
            expect(fs.existsSync(target + '.tmp')).to.be(false);
        });

    });

    describe('safeUnlink', function() {

        it('ignores missing files', function() {
            expect(() => safeUnlink(path.join(testDir, 'does-not-exist'))).to.not.throwError();
        });

    });

    describe('compileAccessor', function() {

        it('returns a direct-property accessor for a single-part path', function() {
            const acc = compileAccessor('type');
            expect(acc({ type: 'OrderPlaced' })).to.be('OrderPlaced');
            expect(acc({})).to.be(undefined);
        });

        it('returns undefined when the object is null or undefined (single-part path)', function() {
            const acc = compileAccessor('type');
            expect(acc(null)).to.be(undefined);
            expect(acc(undefined)).to.be(undefined);
        });

        it('resolves nested dot-notation paths', function() {
            const acc = compileAccessor('payload.user.id');
            expect(acc({ payload: { user: { id: 42 } } })).to.be(42);
        });

        it('returns undefined for missing nested segments', function() {
            const acc = compileAccessor('payload.user.id');
            expect(acc(null)).to.be(undefined);
            expect(acc({})).to.be(undefined);
            expect(acc({ payload: null })).to.be(undefined);
            expect(acc({ payload: { user: 'not-an-object' } })).to.be(undefined);
        });

    });

});
