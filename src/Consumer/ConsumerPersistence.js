/**
 * Base contract for durable consumer persistence adapters.
 *
 * Implementations must provide asynchronous lifecycle and I/O methods.
 */
class ConsumerPersistence {
    /**
     * Prepare external resources before consumer processing starts.
     * @param {object} initialState
     * @param {number} startFrom
     * @returns {Promise<void>}
     */
    async open(initialState, startFrom) {
        throw new Error('ConsumerPersistence.open(initialState, startFrom) must be implemented.');
    }

    /**
     * Load the last persisted snapshot.
     * @param {object} initialState
     * @param {number} startFrom
     * @returns {Promise<{ position: number, state: object }>}
     */
    async load(initialState, startFrom) {
        throw new Error('ConsumerPersistence.load(initialState, startFrom) must be implemented.');
    }

    /**
     * Persist the current consumer position and state atomically.
     * @param {number} position
     * @param {object} state
     * @param {string} consumerName
     * @returns {Promise<unknown>}
     */
    async persist(position, state, consumerName) {
        throw new Error('ConsumerPersistence.persist(position, state, consumerName) must be implemented.');
    }
}

export default ConsumerPersistence;

