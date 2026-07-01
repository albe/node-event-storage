import { assert } from './util.js';

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
 * Compute the difference of two sorted index-entry ranges by global number,
 * that is all entries in `source`, that are not also in `exclude`.
 * R = S \ E
 * Assumes each range is sorted and contains no duplicates (by entry number).
 *
 * @param {Array<Array<number>>} source
 * @param {Array<Array<number>>} exclude
 * @returns {Array<Array<number>>}
 */
function difference(source, exclude) {
    if (source.length === 0) {
        return [];
    }
    if (exclude.length === 0) {
        return source.slice();
    }

    const selected = new Array(source.length);
    let sourceIndex = 0;
    let excludedIndex = 0;
    let count = 0;

    while (sourceIndex < source.length && excludedIndex < exclude.length) {
        const sourceEntry = source[sourceIndex];
        const excludedEntry = exclude[excludedIndex];
        const sourceNumber = sourceEntry[0];
        const excludedNumber = excludedEntry[0];

        if (sourceNumber < excludedNumber) {
            selected[count++] = sourceEntry;
            sourceIndex++;
        } else if (excludedNumber < sourceNumber) {
            excludedIndex++;
        } else {
            sourceIndex++;
            excludedIndex++;
        }
    }

    while (sourceIndex < source.length) {
        selected[count++] = source[sourceIndex++];
    }

    selected.length = count;
    return selected;
}

/**
 * Normalize selector shape and apply structural optimizations (_all and single-node flattening).
 *
 * @param {string|Array<string|Array>|object} selector
 * @param {number} [depth=0]
 * @returns {string|Array<string|Array>|object}
 */
function normalizeSelector(selector, depth = 0) {
    if (typeof selector === 'string') {
        assert(selector.length > 0, 'Stream names must be non-empty strings.');
        return selector;
    }

    if (selector && typeof selector === 'object' && !Array.isArray(selector) && selector.difference) {
        assert(Array.isArray(selector.difference) && selector.difference.length === 2, 'difference must be an array [source, exclude].');
        const [source, exclude] = selector.difference;
        return optimizeSelectorDifference(
            normalizeSelector(source, 0),
            normalizeSelector(exclude, 0)
        );
    }

    assert(Array.isArray(selector), 'Selector must be a string, an array, or a difference object.');
    const normalized = selector.map(node => normalizeSelector(node, depth + 1));
    return optimizeSelectorNode(normalized, depth);
}

function optimizeSelectorDifference(source, exclude) {
    if (Array.isArray(source) && source.length === 0) {
        return [];
    }
    if (Array.isArray(exclude) && exclude.length === 0) {
        return source;
    }
    if (exclude === '_all' || source === exclude) {
        return [];
    }
    if (typeof source === 'string' && Array.isArray(exclude) && exclude.includes(source)) {
        return [];
    }

    return {
        difference: [source, exclude]
    };
}

function optimizeSelectorAndNode(selectorNode) {
    const filtered = selectorNode.filter(node => node !== '_all');
    if (filtered.length < 2) {
        return filtered;
    }

    // ['a', 'b', NOT(X) = { difference: ['_all', X] }] -> { difference: [[['a','b']], X] }
    const positives = [];
    const excludes = [];
    for (const node of filtered) {
        const diff = !Array.isArray(node) && node.difference?.[0];
        if (diff === '_all') {
            excludes.push(node.difference[1]);
        } else {
            positives.push(node);
        }
    }

    if (positives.length === 0 || excludes.length === 0) {
        return filtered;
    }

    const source = positives.length === 1 ? positives[0] : [positives];
    const exclude = excludes.length === 1 ? excludes[0] : excludes;
    return optimizeSelectorDifference(source, exclude);
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
        return optimizeSelectorAndNode(selectorNode);
    }
    if (selectorNode.some(node => node === '_all')) {
        return '_all';
    }
    return selectorNode;
}

export {
    union,
    intersect,
    difference,
    normalizeSelector
};