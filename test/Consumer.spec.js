import expect from 'expect.js';
import fs from 'fs-extra';
import fsNative from 'fs';
import Storage from '../src/Storage.js';
import Consumer from '../src/Consumer.js';
import Projection, { CompositeProjection } from '../src/Projection.js';
import { createHmac } from '../src/utils/metadataUtil.js';
import { fileURLToPath } from 'url';

const __dirname = fileURLToPath(new URL('.', import.meta.url));
const dataDirectory = __dirname + 'data';

describe('Consumer', function() {

    let consumer, storage;

    beforeEach(function () {
        fs.emptyDirSync(dataDirectory);
        storage = new Storage({ dataDirectory });
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
        expect(fs.existsSync(dataDirectory + '/consumers')).to.be(true);
    });

    it('cleans up failed write left-overs', function() {
        consumer = new Consumer(storage, 'foobar', 'consumer1');
        consumer.stop();
        fs.writeFileSync(consumer.fileName + '.1', 'failed write!');
        consumer = new Consumer(storage, 'foobar', 'consumer1');
        expect(fs.existsSync(consumer.fileName + '.1')).to.be(false);
    });

    it('rethrows cleanup unlink errors other than ENOENT', function() {
        consumer = new Consumer(storage, 'foobar', 'consumer1');
        consumer.stop();
        const failedWriteFile = consumer.fileName + '.1';
        fs.writeFileSync(failedWriteFile, 'failed write!');

        const originalUnlinkSync = fsNative.unlinkSync;
        fsNative.unlinkSync = () => {
            const error = new Error('permission denied');
            error.code = 'EACCES';
            throw error;
        };

        try {
            expect(() => consumer.cleanUpFailedWrites()).to.throwError(/permission denied/);
        } finally {
            fsNative.unlinkSync = originalUnlinkSync;
            if (fs.existsSync(failedWriteFile)) {
                fs.unlinkSync(failedWriteFile);
            }
        }
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

    it('can start with some initial state', function(done){
        consumer = new Consumer(storage, 'foobar', 'consumer1', { foos: 0, lastId: 0 });
        expect(consumer.state.foos).to.be(0);
        expect(consumer.state.lastId).to.be(0);
        storage.write({ type: 'Foobar', id: 2 });
        consumer.on('caught-up', () => {
            expect(consumer.state.foos).to.be(1);
            expect(consumer.state.lastId).to.be(2);
            done();
        });
        consumer.on('data', document => {
            if (document.type === 'Foobar') {
                consumer.setState({foos: consumer.state.foos + 1, lastId: document.id});
            }
        });
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

    it('ignores index-add notifications with non-sequential positions', function(done) {
        consumer = new Consumer(storage, 'foobar', 'consumer1');
        consumer.once('caught-up', () => {
            const initialPosition = consumer.position;
            let received = false;

            consumer.once('data', () => {
                received = true;
            });

            storage.emit('index-add', 'foobar', initialPosition + 2, { type: 'Foobar', id: 999 });

            setTimeout(() => {
                expect(received).to.be(false);
                expect(consumer.position).to.be(initialPosition);
                done();
            }, 10);
        });
        consumer.start();
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

    it('will persist multiple setState calls only once', function(done) {
        consumer = new Consumer(storage, 'foobar', 'consumer-1');
        consumer.on('data', () => {
            consumer.setState({ foo: 1 });
            consumer.setState({ foo: 1, bar: 2 });
            consumer.once('persisted', () => {
                expect(consumer.state.bar).to.be(2);
                done();
            });
            consumer.stop();
        });

        storage.write({ type: 'Foobar', id: 1 });
    });

    it('ignores concurrent persist calls while one write is scheduled', function(done) {
        consumer = new Consumer(storage, 'foobar', 'consumer-1');
        consumer.position = 1;
        consumer.state = Object.freeze({ foo: 1 });

        let persistedCount = 0;
        consumer.on('persisted', () => {
            persistedCount++;
        });

        consumer.persist();
        consumer.persist();

        setTimeout(() => {
            expect(persistedCount).to.be(1);
            done();
        }, 15);
    });

    it('swallows persistence write errors and removes temp files', function(done) {
        consumer = new Consumer(storage, 'foobar', 'consumer-1');
        consumer.position = 1;
        consumer.state = Object.freeze({ foo: 1 });

        const tmpFile = consumer.fileName + '.1';
        const originalWriteFileSync = fsNative.writeFileSync;
        fsNative.writeFileSync = () => {
            const error = new Error('disk full');
            error.code = 'ENOSPC';
            throw error;
        };

        consumer.persist();

        setTimeout(() => {
            fsNative.writeFileSync = originalWriteFileSync;
            expect(fs.existsSync(tmpFile)).to.be(false);
            done();
        }, 15);
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

    it('allows function argument in setState', function(done) {
        consumer = new Consumer(storage, 'foobar', 'consumer-1');
        consumer.on('data', (document) => {
            consumer.setState(state => ({...state, foo: (state.foo || 0) + 1, lastId: document.id}));
            consumer.once('persisted', () => {
                expect(consumer.state.foo).to.be(1);
                done();
            });
        });

        storage.write({ type: 'Foobar', id: 1 });
    });

    it('can be reset and reprocesses all events', function(done) {
        storage.write({ type: 'Foobar', id: 1 });
        storage.write({ type: 'Foobar', id: 2 });
        storage.write({ type: 'Foobar', id: 3 });
        consumer = new Consumer(storage, 'foobar', 'consumer-1');
        consumer.once('caught-up', () => {
            consumer.once('caught-up', () => {
                expect(consumer.position).to.be(3);
                done();
            });
            consumer.reset();
            expect(consumer.position).to.be(0);
        });
        consumer.start();
    });

    it('can be reset with new initialState and reprocesses all events', function(done) {
        storage.write({ type: 'Foobar', id: 1 });
        storage.write({ type: 'Foobar', id: 2 });
        storage.write({ type: 'Foobar', id: 3 });
        consumer = new Consumer(storage, 'foobar', 'consumer-1', { foo: 0 });
        consumer.once('caught-up', () => {
            consumer.once('caught-up', () => {
                expect(consumer.state.foo).to.be(4);
                done();
            });
            expect(consumer.state.foo).to.be(3);
            consumer.reset({ foo: 1 });
        });
        consumer.on('data', document => consumer.setState(state => ({ foo: state.foo + 1, lastId: document.id })));
    });

    it('can be reset with new starting point and reprocesses all events', function(done) {
        storage.write({ type: 'Foobar', id: 1 });
        storage.write({ type: 'Foobar', id: 2 });
        storage.write({ type: 'Foobar', id: 3 });
        consumer = new Consumer(storage, 'foobar', 'consumer-1');
        consumer.once('caught-up', () => {
            consumer.once('caught-up', () => {
                expect(consumer.position).to.be(3);
                done();
            });
            consumer.reset(2);
            expect(consumer.position).to.be(2);
        });
        consumer.start();
    });

    it('can be reset while running', function(done) {
        consumer = new Consumer(storage, 'foobar', 'consumer-1');
        consumer.once('caught-up', () => {
            storage.write({ type: 'Foobar', id: 1 });
            storage.write({ type: 'Foobar', id: 2 });
            storage.write({ type: 'Foobar', id: 3 });
        });
        consumer.once('data', () => {
            consumer.reset();
            expect(consumer.position).to.be(0);
            consumer.once('caught-up', () => {
                expect(consumer.position).to.be(3);
                done();
            });
        });
    });

    it('will not restart if stopped before reset', function(done) {
        storage.write({ type: 'Foobar', id: 1 });
        storage.write({ type: 'Foobar', id: 2 });
        storage.write({ type: 'Foobar', id: 3 });
        consumer = new Consumer(storage, 'foobar', 'consumer-1');
        consumer.once('caught-up', () => {
            consumer.stop();
            expect(consumer.isPaused()).to.be(true);
            consumer.reset();
            expect(consumer.isPaused()).to.be(true);
            done();
        });
        consumer.start();
    });

    it('persists state on every setState by default', function(done) {
        consumer = new Consumer(storage, 'foobar', 'consumer-1', { foo: 0 });
        let expected = 0;
        consumer.on('data', document => {
            consumer.setState(state => ({ foo: state.foo + 1, lastId: document.id }));
        });
        consumer.on('persisted', () => {
            expect(consumer.state.lastId).to.be(++expected);
            if (consumer.state.lastId === 3) {
                done();
            } else {
                storage.write({type: 'Foobar', id: consumer.state.lastId + 1});
            }
        });
        consumer.on('caught-up', () => {
            storage.write({ type: 'Foobar', id: 1 });
        });
    });

    it('allows to skip state persistence', function(done) {
        consumer = new Consumer(storage, 'foobar', 'consumer-1', { foo: 0 });
        consumer.on('data', document => {
            consumer.setState(state => ({ foo: state.foo + 1, lastId: document.id }), false);
            if (document.id === 3) {
                expect(consumer.state.foo).to.be(3);
                consumer.stop();

                setTimeout(() => {
                    const consumer = new Consumer(storage, 'foobar', 'consumer-1', { foo: 0 });
                    expect(consumer.state.foo).to.be(0);
                    done();
                }, 1);
            }
        });
        consumer.on('caught-up', () => {
            consumer.on('persisted', () => { throw new Error('Invoked persistence!'); });
            storage.write({ type: 'Foobar', id: 1 });
            storage.write({ type: 'Foobar', id: 2 });
            storage.write({ type: 'Foobar', id: 3 });
        });
    });

    it('can attach projections from a reducer function', function(done) {
        consumer = new Consumer(storage, 'foobar', 'consumer-projection', { count: 0 });
        new Projection('consumer-projection', {
            initialState: { count: 0 },
            handlers: (state, event) => ({ ...state, count: state.count + event.id })
        }, {
            hmac: createHmac('test-secret')
        }).subscribe(consumer);
        consumer.on('caught-up', () => {
            expect(consumer.state.count).to.be(6);
            done();
        });

        storage.write({ type: 'Foobar', id: 1 });
        storage.write({ type: 'Foobar', id: 2 });
        storage.write({ type: 'Foobar', id: 3 });
    });

    it('can attach and restore projections from event-type reducer maps', function(done) {
        consumer = new Consumer(storage, 'foobar', 'consumer-projection-map', { count: 0 });
        const projection = new Projection('consumer-projection-map', {
            initialState: { count: 0 },
            handlers: {
                Foobar: (state, event) => ({ ...state, count: state.count + event.id }),
                Bazinga: (state) => state
            }
        }, { hmac: createHmac('test-secret') });
        projection.subscribe(consumer);

        consumer.on('caught-up', () => {
            consumer.stop();
            consumer = new Consumer(storage, 'foobar', 'consumer-projection-map', {});
            Projection.restoreFromFile(`${consumer.fileName}.projection`, {
                hmac: createHmac('test-secret')
            }).subscribe(consumer);
            consumer.on('progress', () => {
                if (consumer.state.count === 10) {
                    done();
                }
            });
            storage.write({ type: 'Foobar', id: 4 });
        });

        storage.write({ type: 'Foobar', id: 1 });
        storage.write({ type: 'Foobar', id: 2 });
        storage.write({ type: 'Foobar', id: 3 });
    });

    it('throws if function projection is restored without trusted hmac', function() {
        consumer = new Consumer(storage, 'foobar', 'consumer-projection-hmac');
        new Projection('consumer-projection-hmac', {
            initialState: {},
            handlers: (state, event) => ({ ...state, lastId: event.id })
        }, {
            hmac: createHmac('test-secret')
        }).subscribe(consumer);
        expect(() => Projection.restoreFromFile(`${consumer.fileName}.projection`, {
            hmac: createHmac('wrong-secret')
        })).to.throwError(/Invalid HMAC/);
    });

    it('can attach a projection instance and restore it on reopen', function(done) {
        consumer = new Consumer(storage, 'foobar', 'consumer-projection-instance', { count: 0 });
        const projection = new Projection('consumer-projection-instance', {
            initialState: { count: 0 },
            handlers: {
                Foobar: (state, event) => ({ ...state, count: state.count + event.id })
            }
        }, {
            hmac: createHmac('test-secret')
        });
        projection.subscribe(consumer);
        consumer.on('caught-up', () => {
            expect(consumer.state.count).to.be(6);
            consumer.stop();
            consumer = new Consumer(storage, 'foobar', 'consumer-projection-instance', {});
            Projection.restoreFromFile(`${consumer.fileName}.projection`, {
                hmac: createHmac('test-secret')
            }).subscribe(consumer);
            consumer.on('progress', () => {
                if (consumer.state.count === 10) {
                    done();
                }
            });
            storage.write({ type: 'Foobar', id: 4 });
        });
        storage.write({ type: 'Foobar', id: 1 });
        storage.write({ type: 'Foobar', id: 2 });
        storage.write({ type: 'Foobar', id: 3 });
    });

    it('supports composite projections', function() {
        const projection = new CompositeProjection('overview', {
            count: {
                initialState: 0,
                handlers: { Foobar: (state) => state + 1 }
            },
            last: {
                initialState: null,
                handlers: { Foobar: (state, event) => event.id || state }
            }
        });
        projection.handle([{ type: 'Foobar', id: 1 }, { type: 'Foobar', id: 2 }]);
        expect(projection.state).to.eql({ count: 2, last: 2 });
    });

    it('can build consistency guards (aggregates)', function(done) {
        const guard = new Consumer(storage, 'foobar', 'unique-bar-guard');
        guard.apply = function(event) {
            this.setState(state => ({ ...state, ...event }));
        };
        guard.handle = function(command) {
            if (this.state.foo === 'bar') {
                throw new Error('There was already a bar!');
            }
            return {type: 'Foobar', foo: command.foo};
        };
        guard.on('data', guard.apply);

        storage.write(guard.handle({ foo: 'bar' }));

        guard.on('persisted', () => {
            expect(() => storage.write(guard.handle({foo: 'bar'}))).to.throwError(/already a bar/);
            done();
        });
    });

});