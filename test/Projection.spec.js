import expect from 'expect.js';
import fs from 'fs-extra';
import { fileURLToPath } from 'url';
import EventStore, { OptimisticConcurrencyError } from '../src/EventStore.js';
import { createProjection, composeProjections, buildDecisionModel } from '../src/Projection.js';

const __dirname = fileURLToPath(new URL('.', import.meta.url));
const storageDirectory = __dirname + 'data';

const CourseExists = (courseId) => createProjection({
    initialState: false,
    when: { CourseDefined: () => true },
    tagFilter: [`course/${courseId}`]
});
const CourseCapacity = (courseId) => createProjection({
    initialState: 0,
    when: {
        CourseDefined: (state, event) => event.capacity,
        CourseCapacityChanged: (state, event) => event.newCapacity
    },
    tagFilter: [`course/${courseId}`]
});
const CourseSubscriptions = (courseId) => createProjection({
    initialState: 0,
    when: { StudentSubscribedToCourse: (state) => state + 1 },
    tagFilter: [`course/${courseId}`]
});
const StudentSubscriptions = (studentId) => createProjection({
    initialState: 0,
    when: { StudentSubscribedToCourse: (state) => state + 1 },
    tagFilter: [`student/${studentId}`]
});

const CourseSeats = (courseId) => composeProjections({
    capacity: CourseCapacity(courseId),
    taken: createProjection({
        initialState: 0,
        when: {
            StudentSubscribedToCourse: (state) => state + 1,
            StudentUnsubscribedFromCourse: (state) => state - 1
        },
        tagFilter: [`course/${courseId}`]
    })
});

const unsubscribe = (studentId, courseId) => ({
    type: 'StudentUnsubscribedFromCourse', tags: [`course/${courseId}`, `student/${studentId}`]
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

    afterEach(function (done) {
        store.close(() => done());
    });

    it('derives the query from when and tagFilter', function () {
        expect(CourseCapacity('c1').query).to.eql({
            items: [{ types: ['CourseDefined', 'CourseCapacityChanged'], tags: ['course/c1'] }]
        });
        expect(createProjection({ when: { A: () => 1 } }).query).to.eql({ items: [{ types: ['A'] }] });
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

        store.commit('subscriptions', [subscribe('s1', 'c1')], first.commitCondition);
        expect(() => store.commit('subscriptions', [subscribe('s2', 'c1')], second.commitCondition))
            .to.throwError(e => expect(e).to.be.an(OptimisticConcurrencyError));
    });

    it('does not conflict on events outside the consistency boundary', function () {
        store.commit('courses', [defineCourse('c1', 10), defineCourse('c2', 10)]);
        const { commitCondition } = buildDecisionModel(store, { subscriptions: CourseSubscriptions('c1') });

        store.commit('subscriptions', [subscribe('s9', 'c2')]);
        expect(() => store.commit('subscriptions', [subscribe('s1', 'c1')], commitCondition)).to.not.throwError();
    });

    it('updates a decision model with only the events committed since', function () {
        store.commit('courses', [defineCourse('c1', 2)]);
        const seen = [];
        const counting = createProjection({
            initialState: 0,
            when: { StudentSubscribedToCourse: (state, event) => (seen.push(event), state + 1) },
            tagFilter: ['course/c1']
        });
        const first = buildDecisionModel(store, { capacity: CourseCapacity('c1'), subscriptions: counting });

        store.commit('subscriptions', [subscribe('s1', 'c1')]);
        expect(() => store.commit('subscriptions', [subscribe('s2', 'c1')], first.commitCondition))
            .to.throwError(e => expect(e).to.be.an(OptimisticConcurrencyError));

        const updated = first.update();
        expect(updated.state).to.eql({ capacity: 2, subscriptions: 1 });
        expect(seen).to.have.length(1);
        expect(first.state).to.eql({ capacity: 2, subscriptions: 0 });
        expect(updated.commitCondition.noneMatchAfter).to.be(store.length);
        expect(() => store.commit('subscriptions', [subscribe('s2', 'c1')], updated.commitCondition)).to.not.throwError();
    });

    it('returns the same model when nothing was committed since', function () {
        store.commit('courses', [defineCourse('c1', 2)]);
        const model = buildDecisionModel(store, CourseCapacity('c1'));
        expect(model.update()).to.be(model);
    });

    it('keeps an updated model in memory across commands', function () {
        store.commit('courses', [defineCourse('c1', 3)]);
        let model = buildDecisionModel(store, { capacity: CourseCapacity('c1'), subscriptions: CourseSubscriptions('c1') });
        for (const studentId of ['s1', 's2', 's3', 's4']) {
            model = model.update();
            if (model.state.subscriptions >= model.state.capacity) break;
            store.commit('subscriptions', [subscribe(studentId, 'c1')], model.commitCondition);
        }
        expect(model.update().state).to.eql({ capacity: 3, subscriptions: 3 });
    });

    it('returns an immutable model', function () {
        store.commit('courses', [defineCourse('c1', 2)]);
        const model = buildDecisionModel(store, CourseCapacity('c1'));
        expect(Object.isFrozen(model)).to.be(true);
    });

    describe('since', function () {

        it('returns the events that block a commit with the model\'s condition', function () {
            store.commit('courses', [defineCourse('c1', 2), defineCourse('c2', 2)]);
            const model = buildDecisionModel(store, CourseSubscriptions('c1'));
            store.commit('subscriptions', [subscribe('s1', 'c2'), subscribe('s2', 'c1'), subscribe('s3', 'c1')]);

            const blocking = [];
            model.since().forEach((event, metadata) => blocking.push(event.tags[1]));
            expect(blocking).to.eql(['student/s2', 'student/s3']);
        });

        it('is empty when nothing relevant was committed', function () {
            store.commit('courses', [defineCourse('c1', 2)]);
            const model = buildDecisionModel(store, CourseSubscriptions('c1'));
            store.commit('subscriptions', [subscribe('s1', 'c2')]);
            expect([...model.since()]).to.have.length(0);
        });

        it('returns only the events conflicting with a narrowed condition', function () {
            store.commit('courses', [defineCourse('c1', 2)]);
            const model = buildDecisionModel(store, CourseSeats('c1'));
            store.commit('subscriptions', [unsubscribe('s1', 'c1'), subscribe('s2', 'c1')]);
            const types = [...model.since(model.commitCondition.without('StudentUnsubscribedFromCourse'))].map(e => e.type);
            expect(types).to.eql(['StudentSubscribedToCourse']);
        });
    });

    describe('commitCondition.without', function () {

        it('ignores concurrent events of the excluded types', function () {
            store.commit('courses', [defineCourse('c1', 1)]);
            store.commit('subscriptions', [subscribe('s1', 'c1')]);
            const model = buildDecisionModel(store, CourseSeats('c1'));
            expect(model.state).to.eql({ capacity: 1, taken: 1 });

            store.commit('subscriptions', [unsubscribe('s1', 'c1')]);
            expect(() => store.commit('subscriptions', [subscribe('s2', 'c1')], model.commitCondition))
                .to.throwError(e => expect(e).to.be.an(OptimisticConcurrencyError));
            expect(() => store.commit('subscriptions', [subscribe('s2', 'c1')],
                model.commitCondition.without('StudentUnsubscribedFromCourse'))).to.not.throwError();
        });

        it('still conflicts on the remaining types', function () {
            store.commit('courses', [defineCourse('c1', 2)]);
            const model = buildDecisionModel(store, CourseSeats('c1'));
            store.commit('subscriptions', [subscribe('s1', 'c1')]);
            expect(() => store.commit('subscriptions', [subscribe('s2', 'c1')],
                model.commitCondition.without('StudentUnsubscribedFromCourse')))
                .to.throwError(e => expect(e).to.be.an(OptimisticConcurrencyError));
        });

        it('keeps the original condition unchanged and accepts arrays', function () {
            store.commit('courses', [defineCourse('c1', 2)]);
            const { commitCondition } = buildDecisionModel(store, CourseSeats('c1'));
            const narrowed = commitCondition.without(['StudentUnsubscribedFromCourse', 'CourseDefined']);
            expect(narrowed.noneMatchAfter).to.be(commitCondition.noneMatchAfter);
            expect(JSON.stringify(narrowed.selector)).to.not.contain('StudentUnsubscribedFromCourse');
            expect(JSON.stringify(narrowed.selector)).to.not.contain('CourseDefined');
            expect(JSON.stringify(commitCondition.selector)).to.contain('StudentUnsubscribedFromCourse');
        });

        it('throws when no stream would be left', function () {
            store.commit('courses', [defineCourse('c1', 2)]);
            const { commitCondition } = buildDecisionModel(store, CourseSubscriptions('c1'));
            expect(() => commitCondition.without('StudentSubscribedToCourse')).to.throwError(/no streams/);
        });

        it('works for conditions created from a selector', function () {
            store.commit('courses', [defineCourse('c1', 2)]);
            const { condition } = store.query(['CourseDefined', 'CourseCapacityChanged']);
            store.commit('courses', [{ type: 'CourseCapacityChanged', courseId: 'c1', capacity: 3, tags: ['course/c1'] }]);
            expect(() => store.commit('courses', [defineCourse('c2', 2)], condition.without('CourseCapacityChanged'))).to.not.throwError();
        });

        it('does not narrow query items without types', function () {
            store.commit('courses', [defineCourse('c1', 2)]);
            const { condition } = store.query({ items: [{ tags: ['course/c1'] }] });
            store.commit('courses', [defineCourse('c1', 3)]);
            expect(() => store.commit('courses', [defineCourse('c1', 4)], condition.without('CourseDefined')))
                .to.throwError(e => expect(e).to.be.an(OptimisticConcurrencyError));
        });
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
