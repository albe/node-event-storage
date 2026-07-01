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

export {
    union,
    intersect
};

