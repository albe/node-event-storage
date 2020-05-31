const TIME_BASE = process.hrtime();
const DATE_BASE_US = Date.now() * 1000.0 + 999.0; // DATE_BASE_US should match the TIME_BASE with lower resolution, but never be less
const US_PER_SEC = 1.0e6;
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
        this.epoch = Math.floor((epoch instanceof Date ? epoch.getTime() : Number(epoch)) * 1000.0);
    }

    /**
     * @returns {number} The number of microseconds since the epoch given in the constructor. The decimal part denotes the accuracy of the clock in milliseconds.
     * @note Needs to allow at least tens of ms accuracy, better hundreds of ms
     */
    time() {
        const delta = process.hrtime(TIME_BASE);
        return DATE_BASE_US - this.epoch + (delta[0] * US_PER_SEC + Math.floor(delta[1] / 1000)) + (CLOCK_ACCURACY_US / 1000.0);
    }

}

module.exports = Clock;