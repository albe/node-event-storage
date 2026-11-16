import expect from 'expect.js';
import fs from 'fs-extra';
import { fileURLToPath } from 'url';
import EventStore, { OptimisticConcurrencyError } from '../src/EventStore.js';
import { createProjection, composeProjections, buildDecisionModel } from '../src/Projection.js';

const __dirname = fileURLToPath(new URL('.', import.meta.url));
const storageDirectory = __dirname + 'data';

const CourseExists = (courseId) => createProjection({
    initialState: false,
    handlers: { CourseDefined: () => true },
    tagFilter: [`course/${courseId}`]
});
const CourseCapacity = (courseId) => createProjection({
    initialState: 0,
    handlers: {
        CourseDefined: (state, event) => event.capacity,
        CourseCapacityChanged: (state, event) => event.newCapacity
    },
    tagFilter: [`course/${courseId}`]
});
const CourseSubscriptions = (courseId) => createProjection({
    initialState: 0,
    handlers: { StudentSubscribedToCourse: (state) => state + 1 },
    tagFilter: [`course/${courseId}`]
});
const StudentSubscriptions = (studentId) => createProjection({
    initialState: 0,
    handlers: { StudentSubscribedToCourse: (state) => state + 1 },
    tagFilter: [`student/${studentId}`]
});

const defineCourse = (id, capacity) => ({ type: 'CourseDefined', capacity, tags: [`course/${id}`] });
const subscribe = (studentId, courseId) => ({
    type: 'StudentSubscribedToCourse', tags: [`course/${courseId}`, `student/${studentId}`]
});

describe('Projection', function () {

    let store;

    beforeEach(function (done) {
        fs.emptyDirSync(storageDirectory);
        store = new EventStore({ storageDirectory, typeAccessor: 'type', tagsAccessor: 'tags' });
        store.once('ready', done);
    });

    afterEach(function () {
        store.close();
    });

    it('derives the query from handlers and tagFilter', function () {
        expect(CourseCapacity('c1').query).to.eql({
            items: [{ types: ['CourseDefined', 'CourseCapacityChanged'], tags: ['course/c1'] }]
        });
        expect(createProjection({ handlers: { A: () => 1 } }).query).to.eql({ items: [{ types: ['A'] }] });
    });

    it('composes state and query of several projections', function () {
        const composed = composeProjections({ exists: CourseExists('c1'), subs: StudentSubscriptions('s1') });
        expect(composed.initialState).to.eql({ exists: false, subs: 0 });
        expect(composed.query.items).to.have.length(2);
    });

    it('routes each event only to the projections it matches', function () {
        store.commit('courses', [defineCourse('c1', 2), defineCourse('c2', 5)]);
        store.commit('subscriptions', [subscribe('s1', 'c1'), subscribe('s1', 'c2'), subscribe('s2', 'c1')]);

        const { state } = buildDecisionModel(store, {
            exists: CourseExists('c1'),
            capacity: CourseCapacity('c1'),
            courseSubscriptions: CourseSubscriptions('c1'),
            studentSubscriptions: StudentSubscriptions('s1')
        });
        expect(state).to.eql({ exists: true, capacity: 2, courseSubscriptions: 2, studentSubscriptions: 2 });
    });

    it('accepts a single projection instead of a map', function () {
        store.commit('courses', [defineCourse('c1', 2), { type: 'CourseCapacityChanged', newCapacity: 7, tags: ['course/c1'] }]);
        const { state } = buildDecisionModel(store, CourseCapacity('c1'));
        expect(state).to.be(7);
    });

    it('returns an append condition that rejects a conflicting concurrent decision', function () {
        store.commit('courses', [defineCourse('c1', 1)]);
        const decide = (studentId) => buildDecisionModel(store, {
            capacity: CourseCapacity('c1'),
            subscriptions: CourseSubscriptions('c1')
        });

        const first = decide('s1');
        const second = decide('s2');
        expect(first.state).to.eql({ capacity: 1, subscriptions: 0 });

        store.commit('subscriptions', [subscribe('s1', 'c1')], first.appendCondition);
        expect(() => store.commit('subscriptions', [subscribe('s2', 'c1')], second.appendCondition))
            .to.throwError(e => expect(e).to.be.an(OptimisticConcurrencyError));
    });

    it('does not conflict on events outside the consistency boundary', function () {
        store.commit('courses', [defineCourse('c1', 10), defineCourse('c2', 10)]);
        const { appendCondition } = buildDecisionModel(store, { subscriptions: CourseSubscriptions('c1') });

        store.commit('subscriptions', [subscribe('s9', 'c2')]);
        expect(() => store.commit('subscriptions', [subscribe('s1', 'c1')], appendCondition)).to.not.throwError();
    });

    it('continues from a previous decision model with only the new events', function () {
        store.commit('courses', [defineCourse('c1', 2)]);
        const projections = () => ({ capacity: CourseCapacity('c1'), subscriptions: CourseSubscriptions('c1') });
        const first = buildDecisionModel(store, projections());

        store.commit('subscriptions', [subscribe('s1', 'c1')]);
        expect(() => store.commit('subscriptions', [subscribe('s2', 'c1')], first.appendCondition))
            .to.throwError(e => expect(e).to.be.an(OptimisticConcurrencyError));

        const seen = [];
        const counting = createProjection({
            initialState: 0,
            handlers: { StudentSubscribedToCourse: (state, event) => (seen.push(event), state + 1) },
            tagFilter: ['course/c1']
        });
        const retry = buildDecisionModel(store, { capacity: CourseCapacity('c1'), subscriptions: counting }, first);
        expect(retry.state).to.eql({ capacity: 2, subscriptions: 1 });
        expect(seen).to.have.length(1);
        expect(retry.appendCondition.noneMatchAfter).to.be(store.length);
        expect(() => store.commit('subscriptions', [subscribe('s2', 'c1')], retry.appendCondition)).to.not.throwError();
    });

    it('returns the previous state when nothing new was committed', function () {
        store.commit('courses', [defineCourse('c1', 2)]);
        const first = buildDecisionModel(store, CourseCapacity('c1'));
        const again = buildDecisionModel(store, CourseCapacity('c1'), first);
        expect(again.state).to.be(2);
        expect(again.appendCondition.noneMatchAfter).to.be(first.appendCondition.noneMatchAfter);
    });

    it('rejects a previous decision model built from different projections', function () {
        store.commit('courses', [defineCourse('c1', 2)]);
        const first = buildDecisionModel(store, CourseCapacity('c1'));
        expect(() => buildDecisionModel(store, CourseCapacity('c2'), first)).to.throwError(/different projections/);
    });

    it('requires a typeAccessor', function (done) {
        store.close();
        fs.emptyDirSync(storageDirectory);
        store = new EventStore({ storageDirectory });
        store.once('ready', () => {
            expect(() => buildDecisionModel(store, CourseExists('c1'))).to.throwError(/typeAccessor/);
            done();
        });
    });
});
