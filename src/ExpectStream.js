const ExpectedVersion = {
    Any: -1,
    EmptyStream: 0
};

/**
 * DCB accept condition captured by EventStore.query().
 *
 * Pass this object as expectedVersion to EventStore.commit() to reject writes when
 * matching events were appended after `noneMatchAfter`.
 */
class CommitCondition {
    /**
     * @param {string[]} types
     * @param {(function(object, object): boolean)|object|null} [matcher]
     * @param {number} noneMatchAfter
     * @param {boolean} [raw=false]
     */
    constructor(types, matcher = null, noneMatchAfter, raw = false) {
        this.types = types;
        this.matcher = matcher;
        this.raw = raw;
        this.noneMatchAfter = noneMatchAfter;
    }
}

/**
 * Check if a value is an explicit expected-version marker.
 * Used to distinguish between version constraints and other arguments in overloaded commit() calls.
 */
function isExpectedVersionMarker(value) {
    return value instanceof CommitCondition ||
        value instanceof ExpectedStreamVersion ||
        value instanceof ExpectedGlobalSequenceNumber;
}

/**
 * ExpectStream provides factory methods for creating optimistic-concurrency expectations
 * used in commit() calls. Each method returns a strongly-typed expectation object that
 * encodes whether to check stream-local version, global sequence number, or a DCB condition.
 */

/**
 * Represents the expectation that a stream is at a specific stream version (1-based).
 * Used for classic single-stream OCC.
 *
 * @example
 * store.commit('order-42', [event], ExpectStream.AtVersion(5));
 */
class ExpectedStreamVersion {
    constructor(streamVersion) {
        this.streamVersion = streamVersion;
    }
}

/**
 * Represents the expectation that the last event in a stream has a global sequence number.
 * Prevents global write serialization: only checks the target stream's last event.
 *
 * @example
 * store.commit('order-42', [event], ExpectStream.AtGlobalSequence(12345));
 */
class ExpectedGlobalSequenceNumber {
    constructor(sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }
}

/**
 * Represents no optimistic-concurrency check: accept any stream version.
 *
 * @example
 * store.commit('order-42', [event], ExpectStream.Any());
 */
class ExpectedAny extends ExpectedStreamVersion {
    constructor() {
        super(-1);
    }
}

/**
 * Represents the expectation that a stream is empty (version 0).
 *
 * @example
 * store.commit('order-42', [event], ExpectStream.Empty());
 */
class ExpectedEmpty extends ExpectedStreamVersion {
    constructor() {
        super(0);
    }
}

/**
 * Represents a DCB (Dynamic Consistency Boundary) condition: commit succeeds only if
 * no events matching the condition's types (and optional matcher) have been appended
 * since the condition was captured.
 *
 * @example
 * const { condition } = store.query(['OrderPlaced', 'OrderShipped'], matcher);
 * store.commit('order-42', [event], ExpectStream.MatchCondition(condition));
 */
class ExpectedCondition extends CommitCondition {
    constructor(condition) {
        if (!(condition instanceof CommitCondition)) {
            throw new Error('ExpectStream.MatchCondition() requires a CommitCondition instance from query().');
        }
        super(condition.types, condition.matcher, condition.noneMatchAfter, condition.raw ?? false);
    }
}

/**
 * Factory namespace for creating optimistic-concurrency expectations.
 */
const ExpectStream = {
    /**
     * Expect a stream to be at a specific stream version.
     * @param {number} streamVersion The 1-based stream version.
     * @returns {ExpectedStreamVersion}
     */
    AtVersion: (streamVersion) => new ExpectedStreamVersion(streamVersion),

    /**
     * Expect the last event in a stream to have a specific global sequence number.
     * @param {number} sequenceNumber The global sequence number of the last event.
     * @returns {ExpectedGlobalSequenceNumber}
     */
    AtGlobalSequence: (sequenceNumber) => new ExpectedGlobalSequenceNumber(sequenceNumber),

    /**
     * Expect no optimistic-concurrency check (accept any stream version).
     * @returns {ExpectedAny}
     */
    Any: () => new ExpectedAny(),

    /**
     * Expect a stream to be empty (version 0, does not exist yet).
     * @returns {ExpectedEmpty}
     */
    Empty: () => new ExpectedEmpty(),

    /**
     * Expect a DCB condition (from query()) to hold at commit time.
     * @param {CommitCondition} condition The condition produced by `query()`.
     * @returns {ExpectedCondition}
     */
    MatchCondition: (condition) => new ExpectedCondition(condition),
};

export {
    CommitCondition,
    ExpectedVersion,
    ExpectStream,
    ExpectedStreamVersion,
    ExpectedGlobalSequenceNumber,
    ExpectedAny,
    ExpectedEmpty,
    ExpectedCondition,
    isExpectedVersionMarker
};