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
    // Only start the right range when the left one has an entry, so an empty side never touches the other.
    let b = a.done ? a : right.next();
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
 * Resolve a selector tree into one lazy range. Selector levels alternate by depth: OR (union) at even depths,
 * starting at the top level, and AND (intersect) at odd depths. Strings are resolved to ranges by `leafRange`.
 *
 * @param {string|Array<string|Array>} selector
 * @param {function(string): Iterator} leafRange Returns the range for one name, e.g. a stream or a term.
 * @param {boolean} [forwards=true]
 * @param {number} [depth=0]
 * @returns {Iterator}
 */
function resolveSelector(selector, leafRange, forwards = true, depth = 0) {
    if (typeof selector === 'string') {
        return leafRange(selector);
    }
    const ranges = selector.map(node => resolveSelector(node, leafRange, forwards, depth + 1));
    if (ranges.length === 0) {
        return [][Symbol.iterator]();
    }
    const combine = depth % 2 === 0 ? union : intersect;
    return ranges.reduce((left, right) => combine(left, right, forwards));
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
 * Remove streams from a selector by treating them as empty: an OR level drops them, an AND level that contains one
 * matches nothing anymore and is dropped as a whole. The result therefore never matches more than the input.
 * `'_all'` is never removed.
 *
 * @param {string|Array} selector A selector in the algebra of `fromStreams()` (depth 0 = OR).
 * @param {Set<string>} names The stream names to treat as empty.
 * @param {number} [depth=0]
 * @returns {string|Array|null} The reduced selector, or `null` if nothing is left.
 */
function excludeStreams(selector, names, depth = 0) {
    if (typeof selector === 'string') {
        return names.has(selector) && selector !== '_all' ? null : selector;
    }
    const children = selector.map(node => excludeStreams(node, names, depth + 1));
    if (depth % 2 === 0) {
        const remaining = children.filter(node => node !== null);
        return remaining.length > 0 ? remaining : null;
    }
    return children.includes(null) ? null : children;
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
    excludeStreams,
    union,
    intersect,
    resolveSelector,
    normalizeSelector,
    buildStreamSource
};