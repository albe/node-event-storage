import { assert } from './utils/util.js';

/**
 * Creates a set-exclusion marker. Must be used as an argument to anyOf() or allOf().
 * When encountered, ALL preceding arguments in the same call become the source,
 * joined with the parent operator. Multiple items inside without() are also joined
 * with the parent operator, unless an explicit anyOf()/allOf() overrides it.
 *
 *   anyOf(a, without(b))        => a \ b
 *   anyOf(a, b, without(c))     => (a OR b) \ c
 *   anyOf(a, without(b), c)     => (a \ b) OR c
 *   allOf(a, b, without(c))     => (a AND b) \ c
 *   allOf(a, b, without(c, d))  => (a AND b) \ (c AND d)
 *   allOf(a, b, without(anyOf(c, d))) => (a AND b) \ (c OR d)
 */
function without(...args) {
    assert(args.length > 0, 'without() requires at least one argument.');
    return { $without: args };
}

/**
 * Returns the _all stream: matches every event in the store.
 * @returns {'_all'}
 */
function all() {
    return '_all';
}

/**
 * Negation: all events NOT matching the given selectors.
 * not(x) = _all \ x
 */
function not(...args) {
    assert(args.length > 0, 'not() requires at least one argument.');
    const excl = args.length === 1 ? args[0] : { $union: args };
    return { difference: ['_all', excl] };
}

/**
 * OR: matches events present in any of the given streams/selectors.
 * A without() argument applies to the immediately preceding argument.
 *
 * @param {...string|object} args
 */
function anyOf(...args) {
    assert(args.length > 0, 'anyOf() requires at least one argument.');
    return buildWithoutCalls(args, '$union');
}

/**
 * AND: matches events present in all of the given streams/selectors.
 * A without() argument applies to the immediately preceding argument.
 *
 * @param {...string|object} args
 */
function allOf(...args) {
    assert(args.length > 0, 'allOf() requires at least one argument.');
    return buildWithoutCalls(args, '$intersect');
}

function isWithout(node) {
    return node !== null && typeof node === 'object' && !Array.isArray(node) && '$without' in node;
}

function buildWithoutCalls(args, operator) {
    const items = [];
    for (const arg of args) {
        if (isWithout(arg)) {
            assert(items.length > 0, 'without() must follow at least one argument in anyOf()/allOf().');
            // All preceding items become the source, joined with the parent operator.
            const preceding = items.splice(0);
            const source = preceding.length === 1 ? preceding[0] : { [operator]: preceding };
            const excl = arg.$without.length === 1 ? arg.$without[0] : { [operator]: arg.$without };
            items.push({ difference: [source, excl] });
        } else {
            items.push(arg);
        }
    }
    if (items.length === 1) return items[0];
    return { [operator]: items };
}

/**
 * Compile a DSL expression (built with anyOf/allOf/not/without/all) to the
 * internal array-based selector format consumed by JoinEventStream / normalizeSelector.
 *
 * Depth parity: even depth = OR, odd depth = AND.
 * When the operator's parity matches the target depth, children are emitted at depth+1.
 * When it doesn't, a wrapper array shifts the parity, and children are emitted at depth+2.
 *
 * @param {string|object} node
 * @returns {Array}
 */
function compileSelector(node) {
    const compiled = compileNode(node, 0);
    if (Array.isArray(compiled)) return compiled;
    return [compiled];
}

function compileNode(node, depth) {
    if (typeof node === 'string') return node;

    assert(node && typeof node === 'object', 'compileSelector: invalid node (expected string or object).');

    if ('difference' in node) {
        return {
            difference: [
                compileNode(node.difference[0], 0),
                compileNode(node.difference[1], 0)
            ]
        };
    }

    const isUnion = '$union' in node;
    assert(isUnion || '$intersect' in node, 'compileSelector: unknown DSL operator; expected $union or $intersect.');

    const items = isUnion ? node.$union : node.$intersect;
    const depthIsOR = depth % 2 === 0;
    const parityMatches = isUnion === depthIsOR;

    const childDepth = parityMatches ? depth + 1 : depth + 2;
    const compiled = items.map(child => compileNode(child, childDepth));
    const inner = compiled.length === 1 ? compiled[0] : compiled;

    return parityMatches ? inner : [inner];
}

export { anyOf, allOf, without, not, all, compileSelector };








