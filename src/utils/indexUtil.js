import { assert, compileAccessor } from './util.js';
import { buildMatcherFn } from './metadataUtil.js';

/*
 * `union` and `intersect` combine two lazy ranges of index entries, as returned by `index.range()`, into a new
 * lazy range in the same direction. A range may reuse its entry object, so an entry is only read before the range
 * that yielded it is advanced.
 *
 * Calling `next(number)` on a range skips ahead to the first entry that does not come before `number`.
 * `intersect` uses that to jump over long stretches without a match instead of stepping through them.
 */

/**
 * @param {boolean} forwards
 * @returns {function(number, number): boolean} Whether the first number comes before the second in that direction.
 */
function comesBefore(forwards) {
    return forwards ? (left, right) => left < right : (left, right) => left > right;
}

/**
 * Continue yielding the entries of a range, passing skip targets through.
 *
 * @param {IteratorResult} current The current, not yet yielded result of the range.
 * @param {Generator} range
 */
function* drain(current, range) {
    while (!current.done) {
        current = range.next(yield current.value);
    }
}

/**
 * Merge two ranges into one ordered range without duplicates.
 *
 * @param {Generator} left
 * @param {Generator} right
 * @param {boolean} [forwards=true]
 * @returns {Generator}
 */
function* union(left, right, forwards = true) {
    const before = comesBefore(forwards);
    let a = left.next();
    let b = right.next();
    while (!a.done && !b.done) {
        const leftNumber = a.value.number;
        const rightNumber = b.value.number;
        const leftFirst = !before(rightNumber, leftNumber);
        const rightFirst = !before(leftNumber, rightNumber);
        const target = yield leftFirst ? a.value : b.value;
        // Advance the side that was yielded, and also the other side when a skip target leaves it behind.
        if (leftFirst || (target !== undefined && before(leftNumber, target))) {
            a = left.next(target);
        }
        if (rightFirst || (target !== undefined && before(rightNumber, target))) {
            b = right.next(target);
        }
    }
    yield* drain(a, left);
    yield* drain(b, right);
}

/**
 * Yield the entries that are contained in both ranges.
 *
 * @param {Generator} left
 * @param {Generator} right
 * @param {boolean} [forwards=true]
 * @returns {Generator}
 */
function* intersect(left, right, forwards = true) {
    const before = comesBefore(forwards);
    let a = left.next();
    let b = right.next();
    while (!a.done && !b.done) {
        const leftNumber = a.value.number;
        const rightNumber = b.value.number;
        if (leftNumber === rightNumber) {
            const target = yield a.value;
            a = left.next(target);
            b = right.next(target);
        } else if (before(leftNumber, rightNumber)) {
            a = left.next(rightNumber);
        } else {
            b = right.next(leftNumber);
        }
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
    union,
    intersect,
    normalizeSelector,
    buildStreamSource
};