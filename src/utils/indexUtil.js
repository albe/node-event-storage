import { assert, compileAccessor } from './util.js';
import { buildMatcherFn } from './metadataUtil.js';

/**
 * Key of an exhausted cursor. Larger than every real key, so it never wins a minimum and ends every intersection.
 */
const EXHAUSTED = Infinity;

/**
 * Walks the entries of one index between two global sequence numbers, without creating entry objects.
 *
 * All cursors move by ascending `key`. Reading backwards uses the negated sequence number as key, so the
 * union and intersection logic is the same in both directions.
 */
class IndexCursor {

    /**
     * @param {ReadableIndex} index
     * @param {number} minNumber The lowest global sequence number to include.
     * @param {number} maxNumber The highest global sequence number to include.
     * @param {boolean} forwards
     */
    constructor(index, minNumber, maxNumber, forwards) {
        this.index = index;
        this.forwards = forwards;
        this.step = forwards ? 1 : -1;
        const first = index.find(minNumber, true);
        const last = index.find(maxNumber);
        const empty = first === 0 || last === 0 || first > last;
        this.position = forwards ? first : last;
        // One step past the last position to visit.
        this.end = empty ? this.position : (forwards ? last : first) + this.step;
        this.update();
    }

    /**
     * @private
     * @param {number} offset Steps from the current position.
     * @returns {number} The key of the entry at that offset.
     */
    keyAt(offset) {
        const number = this.index.numberAt(this.position + offset * this.step - 1);
        return this.forwards ? number : -number;
    }

    /**
     * @private
     */
    update() {
        this.key = this.position === this.end ? EXHAUSTED : this.keyAt(0);
    }

    next() {
        this.position += this.step;
        this.update();
    }

    /**
     * Move to the first entry whose key is at least `key`.
     * Gallops from the current position before the binary search, so a nearby target costs only a few reads.
     *
     * @param {number} key
     */
    seek(key) {
        if (key <= this.key) {
            return;
        }
        const remaining = (this.end - this.position) * this.step;
        // Invariant: keyAt(low) < key and keyAt(high) >= key, where offset `remaining` counts as the end.
        let low = 0;
        let high = 1;
        while (high < remaining && this.keyAt(high) < key) {
            low = high;
            high = Math.min(2 * high, remaining);
        }
        while (high - low > 1) {
            const mid = low + ((high - low) >> 1);
            if (this.keyAt(mid) < key) {
                low = mid;
            } else {
                high = mid;
            }
        }
        this.position += high * this.step;
        this.update();
    }

    /**
     * @returns {IndexCursor} The index cursor positioned on the current match.
     */
    leaf() {
        return this;
    }
}

/**
 * Union of cursors: the smallest key of all children, each key only once.
 */
class OrCursor {

    /**
     * @param {Array<IndexCursor|OrCursor|AndCursor>} children
     */
    constructor(children) {
        this.children = children;
        this.update();
    }

    /**
     * @private
     */
    update() {
        let min = EXHAUSTED;
        for (const child of this.children) {
            min = Math.min(min, child.key);
        }
        this.key = min;
    }

    next() {
        const key = this.key;
        for (const child of this.children) {
            if (child.key === key) {
                child.next();
            }
        }
        this.update();
    }

    /**
     * @param {number} key
     */
    seek(key) {
        for (const child of this.children) {
            child.seek(key);
        }
        this.update();
    }

    leaf() {
        return this.children.find(child => child.key === this.key).leaf();
    }
}

/**
 * Intersection of cursors: keys all children have. Children skip ahead to the largest key with seek(), so long
 * stretches without a match cost a binary search instead of one step per entry.
 */
class AndCursor {

    /**
     * @param {Array<IndexCursor|OrCursor|AndCursor>} children
     */
    constructor(children) {
        this.children = children;
        this.align();
    }

    /**
     * Seek all children to the largest key until they agree on one.
     *
     * @private
     */
    align() {
        let key = this.children.length > 0 ? this.children[0].key : EXHAUSTED;
        let agreed = false;
        while (!agreed && key !== EXHAUSTED) {
            agreed = true;
            for (const child of this.children) {
                child.seek(key);
                if (child.key !== key) {
                    key = child.key;
                    agreed = false;
                }
            }
        }
        this.key = key;
    }

    next() {
        this.children[0].next();
        this.align();
    }

    /**
     * @param {number} key
     */
    seek(key) {
        this.children[0].seek(key);
        this.align();
    }

    leaf() {
        return this.children[0].leaf();
    }
}

/**
 * Normalize selector shape for optimal evaluation.
 *
 * @param {string|Array<string|Array>} selector
 * @param {number} [depth=0]
 * @returns {string|Array<string|Array>}
 */
function normalizeSelector(selector, depth = 0) {
    if (typeof selector === 'string') {
        assert(selector.length > 0, 'Stream names must be non-empty strings.');
        return selector;
    }

    assert(Array.isArray(selector), 'Selector must be a string or an array.');
    const normalized = selector.map(node => normalizeSelector(node, depth + 1));
    return optimizeSelectorNode(normalized, depth);
}

/**
 * Optimize one normalized selector node by depth-dependent rules.
 *
 * @param {Array<string|Array>} selectorNode
 * @param {number} depth
 * @returns {string|Array<string|Array>}
 */
function optimizeSelectorNode(selectorNode, depth) {
    if (selectorNode.length === 0) {
        return [];
    }
    if (selectorNode.length === 1) {
        const child = selectorNode[0];
        // ['a'] -> 'a', [{ ... }] -> { ... }
        if (!Array.isArray(child)) {
            return selectorNode[0];
        }
        // [['a']] -> 'a', but never [['a']] -> ['a']
        else if (child.length === 1) {
            return child[0];
        }
        return selectorNode;
    }
    // ['a', 'a'] -> 'a'
    if (selectorNode.every(node => node === selectorNode[0])) {
        return selectorNode[0];
    }

    if (depth % 2 !== 0) {
        return selectorNode.filter(node => node !== '_all');
    }
    if (selectorNode.some(node => node === '_all')) {
        return '_all';
    }
    return selectorNode;
}

/**
 * Pre-compile a stream source descriptor with a single curried matcher factory:
 * `matcherFn(operator)(value) => objectMatcher`. The scalar and `$has` shapes are compiled
 * once per source so callers can pick the correct one based on the event property type
 * (scalar → no operator, array → `$has`) without rebuilding the closure per event.
 *
 * @param {string} sourcePath Dot-notation payload path (e.g. `'type'`, `'tags'`).
 * @param {function(string): string} nameBuilder Maps each source value to a stream name.
 * @returns {{path: string, accessor: function, nameBuilder: function, matcherFn: function(string|undefined): (function(any): object)}}
 */
function buildStreamSource(sourcePath, nameBuilder) {
    const scalarMatcher = buildMatcherFn(sourcePath);
    const hasMatcher = buildMatcherFn(sourcePath, '$has');
    return {
        path: sourcePath,
        accessor: compileAccessor(sourcePath),
        nameBuilder,
        matcherFn: (operator) => operator === '$has' ? hasMatcher : scalarMatcher
    };
}

export {
    EXHAUSTED,
    IndexCursor,
    OrCursor,
    AndCursor,
    normalizeSelector,
    buildStreamSource
};