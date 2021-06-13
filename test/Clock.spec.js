const expect = require('expect.js');
const Clock = require('../src/Clock');

describe('Clock', function() {

	it('can be constructed with a numeric clock timestamp', function() {
		const now = Date.now();
		const clock = new Clock(now);
		expect(clock.time()).to.be.greaterThan(1);
	});

	it('can be constructed with a Date', function() {
		const now = new Date();
		const clock = new Clock(now);
		expect(clock.time()).to.be.greaterThan(1);
	});

	describe('time', function() {

		const MS_FACTOR = 1000.0;

		it('returns a microseconds timestamp', function() {
			const oneHourMs = 3600 * 1000;
			const clock = new Clock(Date.now() - oneHourMs);
			const time = clock.time() / MS_FACTOR;
			expect(time).to.be.within(oneHourMs, oneHourMs + 10);
		});

		it('returns a monotonic timestamp', function() {
			const now = Date.now();
			const clock = new Clock(now);
			const times = new Array(100);
			for (let i=0;i<100;i++) {
				times[i] = clock.time();
			}
			for (let i=1;i<times.length;i++){
				expect(times[i]).to.be.greaterThan(times[i-1]);
			}
		});

		it('measures at least 100 years', function() {
			const HundredYearsInMs = 100 * 365 * 24 * 3600 * 1000;
			const clock = new Clock(Date.now() - HundredYearsInMs);
			const time = clock.time() / MS_FACTOR;
			expect(time).to.be.within(HundredYearsInMs, HundredYearsInMs + 10);

			const times = new Array(100);
			for (let i=0;i<100;i++) {
				times[i] = clock.time();
			}
			for (let i=1;i<times.length;i++){
				expect(times[i]).to.be.greaterThan(times[i-1]);
			}
		});

	});

	describe('accuracy', function() {

		it('returns the accuracy of the clock in ms', function() {
			const HundredYearsInMs = 100 * 365 * 24 * 3600 * 1000;
			const clock = new Clock(Date.now() - HundredYearsInMs);

			expect(clock.accuracy(clock.time())).to.be.greaterThan(0);
			expect(clock.accuracy(clock.time())).to.be.lessThan(100);
		});

	});

});
