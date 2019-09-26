const expect = require('expect.js');
const fs = require('fs-extra');
const path = require('path');
const Watcher = require('../src/Watcher');

const dataDirectory = __dirname + '/data';

describe('Watcher', function() {

    let watchers = [];

    beforeEach(function() {
        fs.emptyDirSync(dataDirectory);
    });

    afterEach(function() {
        watchers.forEach(watcher => watcher.close());
        watchers = [];
    });

    function createWatcher(fileOrDirectory, filter) {
        const watcher = new Watcher(path.resolve(dataDirectory, fileOrDirectory), filter);
        watchers.push(watcher);
        return watcher;
    }

    it('throws if file doesnt exist', function(){
        expect(() => createWatcher('.file')).to.throwError();
    });

    it('detects changes to files inside a directory', function(done){
        const fd = fs.openSync(dataDirectory + '/.file', 'w');
        const watcher = createWatcher('');
        watcher.on('change', (filename) => {
            expect(filename).to.be('.file');
            fs.closeSync(fd);
            done();
        });
        fs.write(fd, 'foobar', () => fs.fdatasync(fd));
    });

    it('detects file creations inside a directory', function(done){
        let fd;
        const watcher = createWatcher('');
        watcher.on('rename', (filename) => {
            expect(filename).to.be('.file');
            fs.closeSync(fd);
            done();
        });
        fd = fs.openSync(dataDirectory + '/.file', 'w');
    });

    it('detects file renames inside a directory', function(done){
        let fd = fs.openSync(dataDirectory + '/.file', 'w');
        fs.closeSync(fd);
        const watcher = createWatcher('');
        let count = 0;
        watcher.on('rename', (filename) => {
            count++;
            if (count === 1) {
                expect(filename).to.be('.file');
            } else if (count === 2) {
                expect(filename).to.be('.file2');
                done();
            }
        });
        fs.renameSync(dataDirectory + '/.file', dataDirectory + '/.file2');
    });

    it('can watch a single file', function(done){
        const fd  = fs.openSync(dataDirectory + '/.file', 'w');
        const fd2  = fs.openSync(dataDirectory + '/.file2', 'w');
        const watcher = createWatcher('.file');
        watcher.on('change', (filename) => {
            expect(filename).to.be('.file');
            fs.closeSync(fd);
            fs.closeSync(fd2);
            done();
        });
        fs.write(fd2, 'foobar', () => fs.fdatasync(fd2));
        fs.write(fd, 'foobar', () => fs.fdatasync(fd));
    });

    it('can create multiple instances', function(done){
        const fd  = fs.openSync(dataDirectory + '/.file', 'w');
        const watcher1 = createWatcher('.file');
        const watcher2 = createWatcher('.file');
        let events = 0;
        watcher1.on('change', (filename) => {
            expect(filename).to.be('.file');
            if (++events === 2) {
                fs.closeSync(fd);
                done();
            }
        });
        watcher2.on('change', (filename) => {
            expect(filename).to.be('.file');
            if (++events === 2) {
                fs.closeSync(fd);
                done();
            }
        });
        fs.write(fd, 'foobar', () => fs.fdatasync(fd));
    });

});
