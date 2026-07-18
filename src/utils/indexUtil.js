import { assert, compileAccessor } from './util.js';
import { buildMatcherFn } from './metadataUtil.js';

/**
 * Merge two sorted index-entry ranges into one sorted union without duplicates.
 * Assumes each range is sorted and contains no duplicates (by entry number).
 *
 * @param {Array<Array<number>>} left Sorted index-entry range.
 * @param {Array<Array<number>>} right Sorted index-entry range.
 * @returns {Array<Array<number>>} A new union range.
 */
function unionTwoIndexEntryRanges(left, right) {
    if (left.length === 0) {
        return right.slice();
    }
    if (right.length === 0) {
        return left.slice();
    }

    const merged = new Array(left.length + right.length);
    let leftIndex = 0;
    let rightIndex = 0;
    let count = 0;

    while (leftIndex < left.length && rightIndex < right.length) {
        const leftEntry = left[leftIndex];
        const rightEntry = right[rightIndex];
        const leftNumber = leftEntry[0];
        const rightNumber = rightEntry[0];

        if (leftNumber < rightNumber) {
            merged[count++] = leftEntry;
            leftIndex++;
        } else if (rightNumber < leftNumber) {
            merged[count++] = rightEntry;
            rightIndex++;
        } else {
            merged[count++] = leftEntry;
            leftIndex++;
            rightIndex++;
        }
    }

    while (leftIndex < left.length) {
        merged[count++] = left[leftIndex++];
    }
    while (rightIndex < right.length) {
        merged[count++] = right[rightIndex++];
    }

    merged.length = count;
    return merged;
}

/**
 * Merge multiple sorted index-entry ranges into one sorted union without duplicates.
 * Pairwise reduction via two-way merges, optimized for realistic data with moderate overlap.
 * Assumes each range is sorted and contains no duplicates (by entry number).
 *
 * @param {...Array<Array<number>>} ranges Sorted index-entry ranges.
 * @returns {Array<Array<number>>} A new union range.
 */
function union(...ranges) {
    if (ranges.length === 0) {
        return [];
    }
    if (ranges.length === 1) {
        return ranges[0].slice();
    }

    let merged = unionTwoIndexEntryRanges(ranges[0], ranges[1]);
    for (let i = 2; i < ranges.length; i++) {
        merged = unionTwoIndexEntryRanges(merged, ranges[i]);
    }
    return merged;
}

/**
 * Intersect two sorted index-entry ranges by global number.
 * Assumes each range is sorted and contains no duplicates (by entry number).
 *
 * @param {Array<Array<number>>} left Sorted index-entry range.
 * @param {Array<Array<number>>} right Sorted index-entry range.
 * @returns {Array<Array<number>>} A new intersection range.
 */
function intersectTwoIndexEntryRanges(left, right) {
    if (left.length === 0 || right.length === 0) {
        return [];
    }

    const selected = new Array(Math.min(left.length, right.length));
    let leftIndex = 0;
    let rightIndex = 0;
    let count = 0;

    while (leftIndex < left.length && rightIndex < right.length) {
        const leftEntry = left[leftIndex];
        const rightEntry = right[rightIndex];
        const leftNumber = leftEntry[0];
        const rightNumber = rightEntry[0];

        if (leftNumber < rightNumber) {
            leftIndex++;
        } else if (rightNumber < leftNumber) {
            rightIndex++;
        } else {
            selected[count++] = leftEntry;
            leftIndex++;
            rightIndex++;
        }
    }

    selected.length = count;
    return selected;
}

/**
 * Intersect multiple sorted index-entry ranges by global number.
 * Pairwise reduction with size-order optimization, best for moderate to high overlap.
 * Assumes each range is sorted and contains no duplicates (by entry number).
 *
 * @param {...Array<Array<number>>} ranges Sorted index-entry ranges.
 * @returns {Array<Array<number>>} A new intersection range.
 */
function intersect(...ranges) {
    if (ranges.length === 0) {
        return [];
    }
    if (ranges.length === 1) {
        return ranges[0].slice();
    }

    ranges.sort((a, b) => a.length - b.length);
    if (ranges[0].length === 0) {
        return [];
    }
    let selected = intersectTwoIndexEntryRanges(ranges[0], ranges[1]);
    for (let i = 2; i < ranges.length && selected.length > 0; i++) {
        selected = intersectTwoIndexEntryRanges(selected, ranges[i]);
    }
    return selected;
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