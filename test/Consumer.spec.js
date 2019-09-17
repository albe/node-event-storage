const expect = require('expect.js');
const fs = require('fs-extra');
const Storage = require('../src/Storage');
const Consumer = require('../src/Consumer');

const dataDir = __dirname + '/data';

describe('Consumer', function() {

    let consumer, storage;

    beforeEach(function () {
        fs.emptyDirSync(dataDir);
        storage = new Storage({ dataDirectory: dataDir });
        storage.ensureIndex('foobar', (doc) => doc.type === 'Foobar');
        storage.ensureIndex('bazinga', (doc) => doc.type === 'Bazinga');
    });

    afterEach(function () {
        if (storage) {
            storage.close();
        }
        storage = null;
        consumer = null;
    });

    it('throws when instanciated without a storage', function() {
        expect(() => new Consumer('foobar', 'consumer1')).to.throwError(/storage/);
    });

    it('throws when instanciated without an index name', function() {
        expect(() => new Consumer(storage)).to.throwError(/index name/);
    });

    it('throws when instanciated without an identifier', function() {
        expect(() => new Consumer(storage, 'foobar')).to.throwError(/identifier/);
    });

    it('creates consumer directory if not existing', function() {
        consumer = new Consumer(storage, 'foobar', 'consumer1');
        expect(fs.existsSync(dataDir + '/consumers')).to.be(true);
    });

    it('emits event when catching up', function(done){
        consumer = new Consumer(storage, 'foobar', 'consumer1');
        consumer.stop();
        storage.write({ type: 'Foobar', id: 1 });
        consumer.on('caught-up', () => {
            expect(consumer.position).to.be(1);
            done();
        });
        consumer.start();
    });

    it('continues emitting data after catching up', function(done){
        consumer = new Consumer(storage, 'foobar', 'consumer1');
        consumer.stop();
        storage.write({ type: 'Foobar', id: 1 });
        consumer.on('caught-up', () => {
            expect(consumer.position).to.be(1);
            storage.write({ type: 'Foobar', id: 2 });
            storage.write({ type: 'Foobar', id: 3 });
        });
        let expected = 0;
        consumer.on('data', document => {
            expect(document.id).to.be(++expected);
            if (document.id === 3) {
                done();
            }
        });
        consumer.start();
    });

    it('receives new documents as they are added', function(done){
        consumer = new Consumer(storage, 'foobar', 'consumer1');
        let expected = 0;
        consumer.on('data', document => {
            expect(document.id).to.be(++expected);
            if (document.id === 3) {
                done();
            }
        });
        storage.write({ type: 'Foobar', id: 1 });
        storage.write({ type: 'Foobar', id: 2 });
        storage.write({ type: 'Foobar', id: 3 });
    });

    it('can start from arbitrary position', function(done){
        consumer = new Consumer(storage, 'foobar', 'consumer1', 2);
        let expected = 3;
        consumer.on('data', document => {
            expect(document.id).to.be(expected);
            done();
        });
        storage.write({ type: 'Foobar', id: 1 });
        storage.write({ type: 'Foobar', id: 2 });
        storage.write({ type: 'Foobar', id: 3 });
    });

    it('stops when pushing fails', function(done){
        consumer = new Consumer(storage, 'foobar', 'consumer1');
        consumer.on('caught-up', () => {
            storage.write({type: 'Foobar', id: 1});
            storage.write({type: 'Foobar', id: 2});
            storage.write({type: 'Foobar', id: 3});
        });
        const push = consumer.push.bind(consumer);
        consumer.push = (doc) => push(doc) && false;
        consumer.on('data', document => {
            expect(document.id).to.be(1);
            setTimeout(done, 10);
        });
    });

    it('ignores events on other streams', function(done){
        consumer = new Consumer(storage, 'foobar', 'consumer1');
        consumer.on('caught-up', () => {
            storage.write({type: 'Foobar', id: 1});
            storage.write({type: 'Foobar', id: 2});
            storage.write({type: 'Bazinga', id: 3});
        });
        let expected = 0;
        consumer.on('data', document => {
            expect(document.id).to.be(++expected);
            if (document.id === 2) {
                setTimeout(done, 10);
            }
        });
    });

    it('works with multiple consumers', function(done){
        consumer = new Consumer(storage, 'foobar', 'consumer1');
        consumer.on('caught-up', () => {
            storage.write({type: 'Foobar', id: 1});
            storage.write({type: 'Foobar', id: 2});
            storage.write({type: 'Bazinga', id: 3});
        });
        const consumer2 = new Consumer(storage, 'bazinga', 'consumer2');
        let expected = 0;
        consumer.on('data', document => {
            expect(document.id).to.be(++expected);
            if (document.id === 2) {
                consumer2.on('data', document => {
                    expect(document.id).to.be(++expected);
                    done();
                });
            }
        });
    });

    it('continues from last position after restart', function(done){
        consumer = new Consumer(storage, 'foobar', 'consumer1');
        let expected = 0;
        consumer.on('data', document => {
            expect(document.id).to.be(++expected);
            if (document.id === 3) {
                consumer.stop();
                storage.write({ type: 'Foobar', id: 4 });
                storage.write({ type: 'Foobar', id: 5 }, () => {
                    expect(consumer.position).to.be(3);
                    consumer.start();
                });
            }
            if (document.id === 5) {
                done();
            }
        });
        storage.write({ type: 'Foobar', id: 1 });
        storage.write({ type: 'Foobar', id: 2 });
        storage.write({ type: 'Foobar', id: 3 });
    });

    it('will automatically start consuming when registering data listener', function(done){
        consumer = new Consumer(storage, 'foobar', 'consumer1');
        let expected = 0;
        //consumer.stop();
        storage.write({ type: 'Foobar', id: 1 });
        storage.write({ type: 'Foobar', id: 2 });
        storage.write({ type: 'Foobar', id: 3 }, () => {
            expect(consumer.position).to.be(0);

            consumer.on('data', document => {
                expect(document.id).to.be(++expected);
                if (document.id === 3) {
                    done();
                }
            });
        });
    });

    it('can stop during catching up', function(done){
        consumer = new Consumer(storage, 'foobar', 'consumer1');
        storage.write({ type: 'Foobar', id: 1 });
        storage.write({ type: 'Foobar', id: 2 });
        storage.write({ type: 'Foobar', id: 3 }, () => {
            expect(consumer.position).to.be(0);
            consumer.start();
            consumer.stop();

            consumer.on('data', document => {
                expect(this).to.be(false);
            });
            setTimeout(done, 10);
        });
    });

    it('stops when push fails during catching up', function(done){
        consumer = new Consumer(storage, 'foobar', 'consumer1');
        const push = consumer.push.bind(consumer);
        consumer.push = (doc) => push(doc) && false;
        storage.write({ type: 'Foobar', id: 1 });
        storage.write({ type: 'Foobar', id: 2 });
        storage.write({ type: 'Foobar', id: 3 }, () => {
            expect(consumer.position).to.be(0);

            consumer.on('data', document => {
                expect(document.id).to.be(1);
                setTimeout(done, 10);
            });
        });
    });

    it('starting manually is no-op after registering data listener', function(done){
        consumer = new Consumer(storage, 'foobar', 'consumer1');
        let expected = 0;
        consumer.on('data', document => {
            expect(document.id).to.be(++expected);
            if (document.id === 3) {
                done();
            }
        });
        consumer.start();
        storage.write({ type: 'Foobar', id: 1 });
        storage.write({ type: 'Foobar', id: 2 });
        storage.write({ type: 'Foobar', id: 3 });
    });

    it('throws when calling setState outside of document handler', function() {
        consumer = new Consumer(storage, 'foobar', 'consumer-1');
        expect(() => consumer.setState({ foo: 'bar' })).to.throwError();
    });

    it('restores state after reopening', function(done) {
        const state = { foo: 0, bar: 'baz' };
        consumer = new Consumer(storage, 'foobar', 'consumer-1');
        consumer.on('data', (document) => {
            const newState = {...state, foo: state.foo + 1, lastId: document.id};
            consumer.setState(newState);
            consumer.once('persisted', () => {
                consumer = new Consumer(storage, 'foobar', 'consumer-1');
                expect(consumer.state.foo).to.be(1);
                done();
            });
            consumer.stop();
        });

        storage.write({ type: 'Foobar', id: 1 });
    });

});