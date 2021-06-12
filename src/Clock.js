const TIME_BASE = process.hrtime.bigint();
const DATE_FACTOR = 1000000n;
const DATE_BASE_NS = BigInt(Date.now() + 1) * DATE_FACTOR - 1n;
const CLOCK_ACCURACY_US = 1; // two process.hrtime() calls take roughly this long, so this is the accuracy we can measure time

/**
 * A simple clock that measures monotonic time elapsed since a specific epoch in microseconds.
 * Hence, in theory, this clock supports providing a unique sequential numbering for up to 1 million events/s,
 * which is far beyond typical write capabilities of SSD drives in single thread scenarios.
 */
class Clock {

    /**
     * @param {Date|number} epoch The epoch to base this clock on, either as a Date or a number of the amount of milliseconds since the unix epoch
     */
    constructor(epoch) {
        this.epoch = BigInt(epoch instanceof Date ? epoch.getTime() : Number(epoch)) * DATE_FACTOR;
        this.lastTime = 0;
    }

    /**
     * @returns {number} The number of microseconds since the epoch given in the constructor.
     * @note Needs to allow at least tenths of ms accuracy, better hundredths of ms
     */
    time() {
        const delta = process.hrtime.bigint() - TIME_BASE;
        const timeSinceEpoch = Number((DATE_BASE_NS - this.epoch + delta) / 1000n);
        return this.lastTime = Math.max(this.lastTime + 1, timeSinceEpoch);
    }

    /**
     * Return the clock accuracy of the given timestamp.
     * @param {number} time A timestamp measured by this clock.
     * @returns {number} The amount of µs accuracy this timestamp has.
     */
    accuracy(time) {
        return CLOCK_ACCURACY_US;
    }

}

module.exports = Clock;