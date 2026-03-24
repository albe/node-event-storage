const expect = require('expect.js');
const RingBuffer = require('../src/Index/RingBuffer');

describe('RingBuffer', function() {

    describe('constructor', function() {

        it('initialises length to 0', function() {
            const rb = new RingBuffer(4);
            expect(rb.length).to.be(0);
        });

        it('exposes the configured capacity', function() {
            const rb = new RingBuffer(8);
            expect(rb.capacity).to.be(8);
        });

        it('clamps capacity to a minimum of 1', function() {
            const rb = new RingBuffer(0);
            expect(rb.capacity).to.be(1);
        });

    });

    describe('windowStart', function() {

        it('is 0 when length <= capacity', function() {
            const rb = new RingBuffer(4);
            rb.add('a'); rb.add('b');
            expect(rb.windowStart).to.be(0);
        });

        it('advances as length exceeds capacity', function() {
            const rb = new RingBuffer(3);
            for (let i = 0; i < 5; i++) rb.add(i);
            // length=5, capacity=3 → windowStart=2
            expect(rb.windowStart).to.be(2);
        });

    });

    describe('add', function() {

        it('returns the new length after each add', function() {
            const rb = new RingBuffer(4);
            expect(rb.add('x')).to.be(1);
            expect(rb.add('y')).to.be(2);
        });

        it('increments length on every add', function() {
            const rb = new RingBuffer(3);
            rb.add('a'); rb.add('b'); rb.add('c'); rb.add('d');
            expect(rb.length).to.be(4);
        });

        it('makes items retrievable via get', function() {
            const rb = new RingBuffer(4);
            rb.add('first');
            rb.add('second');
            expect(rb.get(0)).to.be('first');
            expect(rb.get(1)).to.be('second');
        });

    });

    describe('get', function() {

        it('returns null for an index below windowStart', function() {
            const rb = new RingBuffer(2);
            rb.add('a'); rb.add('b'); rb.add('c'); // window = [1, 2]
            expect(rb.get(0)).to.be(null);
        });

        it('returns null for a slot that has never been set', function() {
            const rb = new RingBuffer(4);
            expect(rb.get(0)).to.be(null);
        });

        it('returns the correct item within the window', function() {
            const rb = new RingBuffer(4);
            ['a', 'b', 'c', 'd', 'e'].forEach(v => rb.add(v));
            // window = [1, 4], item at index 4 = 'e'
            expect(rb.get(4)).to.be('e');
        });

        it('returns null after the item has been evicted by further adds', function() {
            const rb = new RingBuffer(3);
            rb.add('a'); rb.add('b'); rb.add('c'); // window=[0,2]
            rb.add('d');                            // window=[1,3], index 0 evicted
            expect(rb.get(0)).to.be(null);
        });

    });

    describe('set', function() {

        it('stores an item at an in-window index', function() {
            const rb = new RingBuffer(4);
            rb.add(null); // length=1
            rb.set(0, 'hello');
            expect(rb.get(0)).to.be('hello');
        });

        it('ignores writes below windowStart', function() {
            const rb = new RingBuffer(2);
            rb.add('a'); rb.add('b'); rb.add('c'); // window=[1,2]
            rb.set(0, 'overwrite');
            expect(rb.get(0)).to.be(null);
        });

    });

    describe('truncate', function() {

        it('reduces length and evicts stale in-window slots', function() {
            const rb = new RingBuffer(4);
            rb.add('a'); rb.add('b'); rb.add('c');
            rb.truncate(1); // keep only index 0
            expect(rb.length).to.be(1);
            expect(rb.get(1)).to.be(null);
            expect(rb.get(2)).to.be(null);
        });

        it('retains entries below the truncation point', function() {
            const rb = new RingBuffer(4);
            rb.add('a'); rb.add('b'); rb.add('c');
            rb.truncate(2);
            expect(rb.get(0)).to.be('a');
            expect(rb.get(1)).to.be('b');
        });

        it('can grow length (no eviction)', function() {
            const rb = new RingBuffer(4);
            rb.add('a');
            rb.truncate(3); // grow
            expect(rb.length).to.be(3);
            expect(rb.get(0)).to.be('a');
        });

        it('allows re-adding after truncation without returning stale data', function() {
            const rb = new RingBuffer(4);
            rb.add('old-1'); rb.add('old-2'); rb.add('old-3');
            rb.truncate(1); // keep only index 0
            rb.add('new-1'); // now at index 1
            expect(rb.get(1)).to.be('new-1');
        });

    });

    describe('reset', function() {

        it('clears all slots and sets length to 0', function() {
            const rb = new RingBuffer(4);
            rb.add('a'); rb.add('b');
            rb.reset();
            expect(rb.length).to.be(0);
            expect(rb.get(0)).to.be(null);
            expect(rb.get(1)).to.be(null);
        });

        it('allows fresh adds after reset', function() {
            const rb = new RingBuffer(4);
            rb.add('a'); rb.add('b');
            rb.reset();
            rb.add('x');
            expect(rb.get(0)).to.be('x');
            expect(rb.length).to.be(1);
        });

    });

});
