import expect from 'expect.js';
import fs from 'fs-extra';
import fsNative from 'fs';
import Storage from '../src/Storage.js';
import Consumer from '../src/Consumer.js';
import ConsumerPersistence from '../src/Consumer/ConsumerPersistence.js';
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

    it('cleans up failed write left-overs', function(done) {
        consumer = new Consumer(storage, 'foobar', 'consumer1');
        consumer.stop();
        const failedWriteFile = consumer.persistence.fileName + '.1';
        fs.writeFileSync(failedWriteFile, 'failed write!');
        consumer = new Consumer(storage, 'foobar', 'consumer1');
        consumer.once('ready', () => {
            expect(fs.existsSync(failedWriteFile)).to.be(false);
            done();
        });
    });

    it('rethrows cleanup unlink errors other than ENOENT', function(done) {
        consumer = new Consumer(storage, 'foobar', 'consumer1');
        consumer.stop();
        const failedWriteFile = consumer.persistence.fileName + '.1';
        fs.writeFileSync(failedWriteFile, 'failed write!');

        const originalUnlinkSync = fsNative.unlinkSync;
        fsNative.unlinkSync = () => {
            const error = new Error('permission denied');
            error.code = 'EACCES';
            throw error;
        };

        consumer.persistence.cleanUpFailedWrites((error) => {
            fsNative.unlinkSync = originalUnlinkSync;
            try {
                expect(error).to.be.an(Error);
                expect(error.message).to.match(/permission denied/);
                if (fs.existsSync(failedWriteFile)) {
                    fs.unlinkSync(failedWriteFile);
                }
                done();
            } catch (assertionError) {
                done(assertionError);
            }
        });
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
        consumer.once('ready', () => {
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
            }, 40);
        });
    });

    it('swallows persistence write errors and removes temp files', function(done) {
        consumer = new Consumer(storage, 'foobar', 'consumer-1');
        consumer.position = 1;
        consumer.state = Object.freeze({ foo: 1 });

        const tmpFile = consumer.persistence.fileName + '.1';
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
                consumer.once('ready', () => {
                    expect(consumer.state.foo).to.be(1);
                    done();
                });
            });
            consumer.stop();
        });

        storage.write({ type: 'Foobar', id: 1 });
    });

    it('waits for asynchronous persistence opening before consuming', function(done) {
        let opened = false;
        class AsyncOpenPersistence extends ConsumerPersistence {
            async open() {
                return new Promise((resolve) => {
                    setTimeout(() => {
                        opened = true;
                        resolve();
                    }, 10);
                });
            }

            async load(initialState, startFrom) {
                return { position: startFrom, state: initialState };
            }

            async persist(position, state) {
                return JSON.stringify(state);
            }
        }

        const persistence = new AsyncOpenPersistence();

        consumer = new Consumer(storage, 'foobar', 'consumer-open', {}, 0, { persistence });
        consumer.on('data', () => {
            expect(opened).to.be(true);
            done();
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

    it('supports custom persistence adapters', function(done) {
        let loadCalls = 0;
        let persistedPosition = 0;
        let persistedState = null;
        class TestPersistence extends ConsumerPersistence {
            async open() {
            }

            async load(initialState, startFrom) {
                loadCalls++;
                return { position: startFrom, state: initialState };
            }

            async persist(position, state) {
                persistedPosition = position;
                persistedState = state;
                return JSON.stringify(state);
            }
        }

        const persistence = new TestPersistence();

        consumer = new Consumer(storage, 'foobar', 'consumer-custom', { foo: 0 }, 0, { persistence });
        consumer.once('ready', () => {
            expect(loadCalls).to.be(1);
        });

        consumer.on('data', (document) => {
            consumer.setState({ foo: document.id });
            consumer.once('persisted', () => {
                expect(persistedPosition).to.be(1);
                expect(persistedState).to.eql({ foo: 1 });
                done();
            });
        });

        storage.write({ type: 'Foobar', id: 1 });
    });

    it('throws when custom persistence is not a ConsumerPersistence instance', function() {
        expect(() => new Consumer(storage, 'foobar', 'consumer-x', {}, 0, {
            persistence: {
                async load(initialState, startFrom) {
                    return { position: startFrom, state: initialState };
                },
                async persist() {
                    return '{}';
                }
            }
        })).to.throwError(/ConsumerPersistence/);

        expect(() => new Consumer(storage, 'foobar', 'consumer-y', {}, 0, {
            persistence: {
                async open() {
                },
                async persist() {
                    return '{}';
                }
            }
        })).to.throwError(/ConsumerPersistence/);
    });

    it('throws when persistence open() does not return a promise', function() {
        class NonAsyncOpenPersistence extends ConsumerPersistence {
            open() {
            }

            async load(initialState, startFrom) {
                return { position: startFrom, state: initialState };
            }

            async persist(position, state) {
                return JSON.stringify(state);
            }
        }

        expect(() => new Consumer(storage, 'foobar', 'consumer-sync-open', {}, 0, {
            persistence: new NonAsyncOpenPersistence()
        })).to.throwError(/open\(\) must return a Promise/);
    });

    it('throws when persistence persist() does not return a promise', function(done) {
        class NonAsyncPersistPersistence extends ConsumerPersistence {
            async open() {
            }

            async load(initialState, startFrom) {
                return { position: startFrom, state: initialState };
            }

            persist() {
                return '{}';
            }
        }

        consumer = new Consumer(storage, 'foobar', 'consumer-sync-persist', {}, 0, {
            persistence: new NonAsyncPersistPersistence()
        });
        consumer.position = 1;
        consumer.state = Object.freeze({ foo: 1 });
        consumer.once('error', (error) => {
            expect(error.message).to.match(/persist\(\) must return a Promise/);
            done();
        });
        consumer.persist();
    });

    it('awaits asynchronous persist before allowing the next persist', function(done) {
        let activeWrites = 0;
        let maxActiveWrites = 0;
        class AsyncPersistTestPersistence extends ConsumerPersistence {
            async open() {
            }

            async load(initialState, startFrom) {
                return { position: startFrom, state: initialState };
            }

            async persist(position, state) {
                activeWrites++;
                maxActiveWrites = Math.max(maxActiveWrites, activeWrites);
                await new Promise((resolve) => setTimeout(resolve, 15));
                activeWrites--;
                return JSON.stringify(state);
            }
        }

        const persistence = new AsyncPersistTestPersistence();

        consumer = new Consumer(storage, 'foobar', 'consumer-async-persist', {}, 0, { persistence });
        consumer.position = 1;
        consumer.state = Object.freeze({ foo: 1 });
        consumer.persist();
        consumer.persist();

        setTimeout(() => {
            expect(maxActiveWrites).to.be(1);
            done();
        }, 40);
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