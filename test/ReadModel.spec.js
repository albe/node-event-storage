import expect from 'expect.js';
import fs from 'fs-extra';
import { fileURLToPath } from 'url';
import EventStore, { OptimisticConcurrencyError } from '../src/EventStore.js';
import { createProjection, transformEvents } from '../src/Projection.js';
import { createReadModel } from '../src/ReadModel.js';
import { localSource } from '../src/LocalSource.js';
import { fileSnapshotStore } from '../src/FileSnapshotStore.js';

const __dirname = fileURLToPath(new URL('.', import.meta.url));
const storageDirectory = __dirname + 'data';
const snapshotDirectory = storageDirectory + '/readmodels';

const applied = [];
const CourseOverview = createProjection({
    initialState: {},
    when: {
        CourseDefined: (state, event) => (applied.push(event), { ...state, [event.id]: { capacity: event.capacity, subscribed: 0 } }),
        StudentSubscribedToCourse: (state, event) => (applied.push(event), {
            ...state,
            [event.courseId]: { ...state[event.courseId], subscribed: state[event.courseId].subscribed + 1 }
        })
    }
});
const CourseSubscriptions = (courseId) => createProjection({
    initialState: 0,
    when: { StudentSubscribedToCourse: (state) => state + 1 },
    tagFilter: [`course/${courseId}`]
});

const defineCourse = (id, capacity) => ({ type: 'CourseDefined', id, capacity, tags: [`course/${id}`] });
const subscribe = (studentId, courseId) => ({
    type: 'StudentSubscribedToCourse', courseId, studentId, tags: [`course/${courseId}`, `student/${studentId}`]
});

function openStore(config = {}) {
    return new Promise(resolve => {
        const store = new EventStore({ storageDirectory, typeAccessor: 'type', tagsAccessor: 'tags', ...config });
        store.once('ready', () => resolve(store));
    });
}

/**
 * Resolve once the read model reached the given position.
 */
function reached(model, position) {
    if (model.position >= position) return Promise.resolve(model);
    return new Promise(resolve => {
        const check = () => {
            if (model.position >= position) {
                model.removeListener('change', check);
                resolve(model);
            }
        };
        model.on('change', check);
    });
}

const tick = () => new Promise(resolve => setImmediate(resolve));

describe('ReadModel', function () {

    let store, source, readModel;

    beforeEach(async function () {
        fs.emptyDirSync(storageDirectory);
        applied.length = 0;
        store = await openStore();
        source = localSource(store);
    });

    afterEach(async function () {
        await readModel?.close();
        readModel = null;
        store?.close();
    });

    it('builds the state from existing events', async function () {
        store.commit('courses', [defineCourse('c1', 2), defineCourse('c2', 5)]);
        store.commit('subscriptions', [subscribe('s1', 'c1')]);
        readModel = await createReadModel(source, CourseOverview).ready;
        expect(readModel.state).to.eql({ c1: { capacity: 2, subscribed: 1 }, c2: { capacity: 5, subscribed: 0 } });
        expect(readModel.position).to.be(3);
        expect(readModel.caughtUp).to.be(true);
    });

    it('follows new commits once caught up and emits change', async function () {
        readModel = await createReadModel(source, CourseOverview).ready;
        const changes = [];
        readModel.on('change', (state, position) => changes.push(position));
        store.commit('courses', [defineCourse('c1', 2)]);
        store.commit('other', [{ type: 'Unrelated' }]);
        store.commit('subscriptions', [subscribe('s1', 'c1')]);
        await reached(readModel, 3);
        expect(readModel.state.c1).to.eql({ capacity: 2, subscribed: 1 });
        expect(changes).to.eql([1, 3]);
    });

    it('only applies events matching its query', async function () {
        store.commit('courses', [defineCourse('c1', 2), defineCourse('c2', 2)]);
        readModel = await createReadModel(source, CourseSubscriptions('c1')).ready;
        store.commit('subscriptions', [subscribe('s1', 'c2'), subscribe('s2', 'c1')]);
        store.commit('subscriptions', [subscribe('s3', 'c1')]);
        await reached(readModel, 5);
        expect(readModel.state).to.be(2);
        expect(applied).to.have.length(0);
    });

    it('follows streams that are created after it started', async function () {
        readModel = await createReadModel(source, CourseSubscriptions('c9')).ready;
        store.commit('subscriptions', [subscribe('s1', 'c9')]);
        await reached(readModel, 1);
        expect(readModel.state).to.be(1);
    });

    it('stops following after close', async function () {
        readModel = await createReadModel(source, CourseOverview).ready;
        await readModel.close();
        store.commit('courses', [defineCourse('c1', 2)]);
        await tick();
        expect(readModel.state).to.eql({});
    });

    it('rejects ready when closed before it caught up', async function () {
        store.commit('courses', [defineCourse('c1', 2)]);
        readModel = createReadModel(source, CourseOverview);
        const ready = readModel.ready;
        await readModel.close();
        let error;
        try { await ready; } catch (e) { error = e; }
        expect(error.message).to.match(/closed/);
    });

    it('creates a decision model from its current state', async function () {
        store.commit('courses', [defineCourse('c1', 2)]);
        readModel = await createReadModel(source, CourseSubscriptions('c1')).ready;
        store.commit('subscriptions', [subscribe('s1', 'c1')]);
        const model = await readModel.decisionModel();
        expect(model.state).to.be(1);
        store.commit('subscriptions', [subscribe('s2', 'c1')]);
        expect(() => store.commit('subscriptions', [subscribe('s3', 'c1')], model.commitCondition))
            .to.throwError(e => expect(e).to.be.an(OptimisticConcurrencyError));
        expect((await model.update()).state).to.be(2);
    });

    it('applies transformed events', async function () {
        store.commit('courses', [defineCourse('c1', 'secret')]);
        const decrypt = async (event) => event.capacity === 'secret' ? { ...event, capacity: 4 } : event;
        readModel = await createReadModel(transformEvents(source, decrypt), CourseOverview).ready;
        store.commit('courses', [defineCourse('c2', 'secret')]);
        await reached(readModel, 2);
        expect(readModel.state).to.eql({ c1: { capacity: 4, subscribed: 0 }, c2: { capacity: 4, subscribed: 0 } });
    });

    it('requires an event source', function () {
        expect(() => createReadModel(store, CourseOverview)).to.throwError(/event source/);
    });

    describe('catch-up', function () {

        it('yields to the event loop in batches', async function () {
            for (let i = 1; i <= 10; i++) {
                store.commit('courses', [defineCourse('c' + i, i)]);
            }
            readModel = createReadModel(source, CourseOverview, { batchSize: 3 });
            expect(readModel.state).to.eql({});
            expect(readModel.caughtUp).to.be(false);
            const progress = [];
            readModel.on('progress', position => progress.push(position));
            await readModel.ready;
            expect(progress).to.eql([3, 6, 9]);
            expect(readModel.position).to.be(10);
            expect(Object.keys(readModel.state)).to.have.length(10);
        });

        it('applies commits made during the catch-up exactly once', async function () {
            for (let i = 1; i <= 5; i++) {
                store.commit('courses', [defineCourse('c' + i, i)]);
            }
            readModel = createReadModel(source, CourseOverview, { batchSize: 2 });
            readModel.once('progress', () => store.commit('subscriptions', [subscribe('s1', 'c1')]));
            store.commit('subscriptions', [subscribe('s2', 'c1')]);
            await readModel.ready;
            store.commit('subscriptions', [subscribe('s3', 'c1')]);
            await reached(readModel, 8);
            expect(readModel.state.c1.subscribed).to.be(3);
            expect(applied.filter(e => e.type === 'StudentSubscribedToCourse')).to.have.length(3);
        });

        it('rejects an invalid batch size', function () {
            expect(() => createReadModel(source, CourseOverview, { batchSize: 0 })).to.throwError(/batch size/);
        });
    });

    describe('with snapshots', function () {

        let snapshots;

        beforeEach(function () {
            snapshots = fileSnapshotStore(snapshotDirectory);
        });

        function persisted(model) {
            return new Promise(resolve => model.once('persisted', resolve));
        }

        it('requires a name', function () {
            expect(() => createReadModel(source, CourseOverview, { snapshots })).to.throwError(/needs a name/);
        });

        it('rejects names that leave the snapshot directory', function () {
            expect(() => snapshots.load('../../../x')).to.throwError(/leaves/);
        });

        it('restores state and position and only reads newer events', async function () {
            store.commit('courses', [defineCourse('c1', 2)]);
            readModel = await createReadModel(source, CourseOverview, { name: 'overview', snapshots }).ready;
            store.commit('subscriptions', [subscribe('s1', 'c1')]);
            await persisted(readModel);
            await readModel.close();
            store.close();

            store = await openStore();
            source = localSource(store);
            store.commit('subscriptions', [subscribe('s2', 'c1')]);
            applied.length = 0;
            readModel = await createReadModel(source, CourseOverview, { name: 'overview', snapshots }).ready;
            expect(readModel.state).to.eql({ c1: { capacity: 2, subscribed: 2 } });
            expect(readModel.position).to.be(3);
            expect(applied).to.have.length(1);
        });

        it('rebuilds when the version changed', async function () {
            store.commit('courses', [defineCourse('c1', 2)]);
            readModel = await createReadModel(source, CourseOverview, { name: 'overview', snapshots, version: 1 }).ready;
            await readModel.close();
            applied.length = 0;
            readModel = await createReadModel(source, CourseOverview, { name: 'overview', snapshots, version: 2 }).ready;
            expect(applied).to.have.length(1);
            expect(readModel.state).to.eql({ c1: { capacity: 2, subscribed: 0 } });
        });

        it('rebuilds when the query changed', async function () {
            store.commit('subscriptions', [subscribe('s1', 'c1'), subscribe('s2', 'c2')]);
            readModel = await createReadModel(source, CourseSubscriptions('c1'), { name: 'subs', snapshots }).ready;
            await readModel.close();
            readModel = await createReadModel(source, CourseSubscriptions('c2'), { name: 'subs', snapshots }).ready;
            expect(readModel.state).to.be(1);
            expect(readModel.position).to.be(2);
        });

        it('rebuilds when the store is shorter than the snapshot', async function () {
            snapshots.save('overview', { version: null, query: CourseOverview.query, position: 5, state: { stale: true } });
            store.commit('courses', [defineCourse('c1', 2)]);
            readModel = await createReadModel(source, CourseOverview, { name: 'overview', snapshots }).ready;
            expect(readModel.state).to.eql({ c1: { capacity: 2, subscribed: 0 } });
            expect(readModel.position).to.be(1);
        });

        it('saves a pending state on close', async function () {
            readModel = await createReadModel(source, CourseOverview, { name: 'overview', snapshots }).ready;
            store.commit('courses', [defineCourse('c1', 2)]);
            await reached(readModel, 1);
            await readModel.close();
            const snapshot = snapshots.load('overview');
            expect(snapshot.position).to.be(1);
            expect(snapshot.state).to.eql({ c1: { capacity: 2, subscribed: 0 } });
        });

        it('accepts an asynchronous snapshot store', async function () {
            const saved = new Map();
            const memory = {
                load: async (name) => saved.get(name) ?? null,
                save: async (name, snapshot) => { saved.set(name, snapshot); }
            };
            store.commit('courses', [defineCourse('c1', 2)]);
            readModel = await createReadModel(source, CourseOverview, { name: 'overview', snapshots: memory }).ready;
            await readModel.close();
            expect(saved.get('overview').position).to.be(1);
        });
    });

    describe('in a read-only process', function () {

        it('follows events written by the writer', async function () {
            this.timeout(5000);
            await new Promise(resolve => store.commit('courses', [defineCourse('c1', 2)], resolve));
            const reader = await openStore({ readOnly: true });
            try {
                readModel = await createReadModel(localSource(reader), CourseOverview).ready;
                store.commit('subscriptions', [subscribe('s1', 'c1')]);
                await reached(readModel, 2);
                expect(readModel.state.c1).to.eql({ capacity: 2, subscribed: 1 });
                await readModel.close();
                readModel = null;
            } finally {
                reader.close();
            }
        });
    });
});
