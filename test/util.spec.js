const expect = require('expect.js');
const fs = require('fs-extra');
const path = require('path');
const { scanForFiles } = require('../src/util');

const testDir = path.join(__dirname, 'data', 'util-scan-test');

describe('util', function() {

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
            scanForFiles(testDir, /(stream-.*)\.index$/, (match) => {
                found.push(match[1]);
            }, (err) => {
                expect(err).to.be(null);
                expect(found.sort()).to.eql(['stream-bar', 'stream-foo']);
                done();
            });
        });

        it('calls onDone with no matches when no files match', function(done) {
            fs.writeFileSync(path.join(testDir, 'unrelated.txt'), '');

            const found = [];
            scanForFiles(testDir, /(stream-.*)\.index$/, (match) => {
                found.push(match[1]);
            }, (err) => {
                expect(err).to.be(null);
                expect(found).to.eql([]);
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
