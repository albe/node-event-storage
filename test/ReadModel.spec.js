import expect from 'expect.js';
import fs from 'fs-extra';
import { fileURLToPath } from 'url';
import EventStore, { OptimisticConcurrencyError } from '../src/EventStore.js';
import { createProjection } from '../src/Projection.js';
import { createReadModel } from '../src/ReadModel.js';

const __dirname = fileURLToPath(new URL('.', import.meta.url));
const storageDirectory = __dirname + 'data';

const applied = [];
const CourseOverview = createProjection({
    initialState: {},
    handlers: {
        CourseDefined: (state, event) => (applied.push(event), { ...state, [event.id]: { capacity: event.capacity, subscribed: 0 } }),
        StudentSubscribedToCourse: (state, event) => (applied.push(event), {
            ...state,
            [event.courseId]: { ...state[event.courseId], subscribed: state[event.courseId].subscribed + 1 }
        })
    }
});
const CourseSubscriptions = (courseId) => createProjection({
    initialState: 0,
    handlers: { StudentSubscribedToCourse: (state) => state + 1 },
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

describe('ReadModel', function () {

    let store, readModel;

    beforeEach(async function () {
        fs.emptyDirSync(storageDirectory);
        applied.length = 0;
        store = await openStore();
    });

    afterEach(function () {
        readModel?.close();
        readModel = null;
        store?.close();
    });

    it('builds the state from existing events', function () {
        store.commit('courses', [defineCourse('c1', 2), defineCourse('c2', 5)]);
        store.commit('subscriptions', [subscribe('s1', 'c1')]);
        readModel = createReadModel(store, CourseOverview);
        expect(readModel.state).to.eql({ c1: { capacity: 2, subscribed: 1 }, c2: { capacity: 5, subscribed: 0 } });
        expect(readModel.position).to.be(3);
    });

    it('follows new commits synchronously and emits change', function () {
        readModel = createReadModel(store, CourseOverview);
        const changes = [];
        readModel.on('change', (state, position) => changes.push(position));
        store.commit('courses', [defineCourse('c1', 2)]);
        store.commit('other', [{ type: 'Unrelated' }]);
        store.commit('subscriptions', [subscribe('s1', 'c1')]);
        expect(readModel.state.c1).to.eql({ capacity: 2, subscribed: 1 });
        expect(readModel.position).to.be(3);
        expect(changes).to.eql([1, 3]);
    });

    it('only applies events matching its query', function () {
        store.commit('courses', [defineCourse('c1', 2), defineCourse('c2', 2)]);
        readModel = createReadModel(store, CourseSubscriptions('c1'));
        store.commit('subscriptions', [subscribe('s1', 'c2'), subscribe('s2', 'c1')]);
        expect(readModel.state).to.be(1);
    });

    it('stops following after close', function () {
        readModel = createReadModel(store, CourseOverview);
        readModel.close();
        store.commit('courses', [defineCourse('c1', 2)]);
        expect(readModel.state).to.eql({});
        expect(readModel.update().state).to.eql({ c1: { capacity: 2, subscribed: 0 } });
    });

    it('creates a decision model from its current state', function () {
        store.commit('courses', [defineCourse('c1', 2)]);
        readModel = createReadModel(store, CourseSubscriptions('c1'));
        store.commit('subscriptions', [subscribe('s1', 'c1')]);
        const model = readModel.decisionModel();
        expect(model.state).to.be(1);
        store.commit('subscriptions', [subscribe('s2', 'c1')]);
        expect(() => store.commit('subscriptions', [subscribe('s3', 'c1')], model.appendCondition))
            .to.throwError(e => expect(e).to.be.an(OptimisticConcurrencyError));
        expect(model.update().state).to.be(2);
    });

    describe('persisted', function () {

        function persisted(model) {
            return new Promise(resolve => model.once('persisted', resolve));
        }

        it('requires a name', function () {
            expect(() => createReadModel(store, CourseOverview, { persist: true })).to.throwError(/needs a name/);
        });

        it('rejects names that leave the readmodels directory', function () {
            expect(() => createReadModel(store, CourseOverview, { persist: true, name: '../../../x' })).to.throwError(/leaves/);
        });

        it('restores state and position and only reads newer events', async function () {
            store.commit('courses', [defineCourse('c1', 2)]);
            readModel = createReadModel(store, CourseOverview, { name: 'overview', persist: true });
            store.commit('subscriptions', [subscribe('s1', 'c1')]);
            await persisted(readModel);
            readModel.close();
            store.close();

            store = await openStore();
            store.commit('subscriptions', [subscribe('s2', 'c1')]);
            applied.length = 0;
            readModel = createReadModel(store, CourseOverview, { name: 'overview', persist: true });
            expect(readModel.state).to.eql({ c1: { capacity: 2, subscribed: 2 } });
            expect(readModel.position).to.be(3);
            expect(applied).to.have.length(1);
        });

        it('rebuilds when the version changed', async function () {
            store.commit('courses', [defineCourse('c1', 2)]);
            readModel = createReadModel(store, CourseOverview, { name: 'overview', persist: true, version: 1 });
            readModel.close();
            applied.length = 0;
            readModel = createReadModel(store, CourseOverview, { name: 'overview', persist: true, version: 2 });
            expect(applied).to.have.length(1);
            expect(readModel.state).to.eql({ c1: { capacity: 2, subscribed: 0 } });
        });

        it('rebuilds when the query changed', async function () {
            store.commit('subscriptions', [subscribe('s1', 'c1'), subscribe('s2', 'c2')]);
            readModel = createReadModel(store, CourseSubscriptions('c1'), { name: 'subs', persist: true });
            readModel.close();
            readModel = createReadModel(store, CourseSubscriptions('c2'), { name: 'subs', persist: true });
            expect(readModel.state).to.be(1);
            expect(readModel.position).to.be(2);
        });

        it('writes a pending state on close', function () {
            readModel = createReadModel(store, CourseOverview, { name: 'overview', persist: true });
            store.commit('courses', [defineCourse('c1', 2)]);
            readModel.close();
            const snapshot = JSON.parse(fs.readFileSync(readModel.fileName, 'utf8'));
            expect(snapshot.position).to.be(1);
            expect(snapshot.state).to.eql({ c1: { capacity: 2, subscribed: 0 } });
        });
    });

    describe('in a read-only process', function () {

        it('follows events written by the writer', function (done) {
            this.timeout(5000);
            store.commit('courses', [defineCourse('c1', 2)], () => {
            const reader = new EventStore({ storageDirectory, typeAccessor: 'type', tagsAccessor: 'tags', readOnly: true });
            reader.once('ready', () => {
                readModel = createReadModel(reader, CourseOverview);
                readModel.on('change', (state) => {
                    if (state.c1?.subscribed === 1) {
                        expect(state.c1.capacity).to.be(2);
                        readModel.close();
                        readModel = null;
                        reader.close();
                        done();
                    }
                });
                store.commit('subscriptions', [subscribe('s1', 'c1')]);
            });
            });
        });
    });
});
