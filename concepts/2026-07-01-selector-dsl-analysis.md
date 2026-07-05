# Selector DSL Analysis: AST-Approach & `exclude()` Semantics

**Date:** 2026-07-01  
**Scope:** Review of `buildWithExcludes()` implementation and `exclude()` marker naming

---

## Part 1: Intermediate AST Approach

### Current Implementation (Direct Logik in `buildWithExcludes()`)

The present implementation processes `exclude()` markers on-the-fly in a single pass:

```javascript
function buildWithExcludes(args, operator) {
    const items = [];
    for (const arg of args) {
        if (isExclude(arg)) {
            // Logic lives here: splice preceding items, wrap with appropriate operator
            const preceding = items.splice(0);
            const source = preceding.length === 1 ? preceding[0] : { [operator]: preceding };
            const excl = arg.$exclude.length === 1 ? arg.$exclude[0] : { [operator]: arg.$exclude };
            items.push({ difference: [source, excl] });
        } else {
            items.push(arg);
        }
    }
    if (items.length === 1) return items[0];
    return { [operator]: items };
}
```

**Characteristics:**
- Single traversal, inline logic
- Tightly couples parsing intent detection with transformation semantics
- Relies on mutable array state (`splice()`) to track position context
- Efficient for small argument lists; readable for current code size (~15 lines)

---

## Alternative: Intermediate AST (Parse → Normalize → Compile)

A three-phase approach would decouple concerns:

```javascript
// Phase 1: Parse raw args into parse tree, marking $exclude nodes
function parseArgs(args) {
    return args.map(arg => {
        if (isExclude(arg)) {
            return { type: 'exclude', args: arg.$exclude };
        }
        return { type: 'include', arg };
    });
}

// Phase 2: Normalize — coalesce adjacent includes, track exclude scope
function normalizeParseTree(parseTree) {
    const groups = [];
    let currentIncludes = [];
    
    for (const node of parseTree) {
        if (node.type === 'include') {
            currentIncludes.push(node.arg);
        } else {
            // exclude: emit preceding includes + the difference
            if (currentIncludes.length > 0) {
                groups.push({ type: 'include-group', items: currentIncludes });
                currentIncludes = [];
            }
            groups.push(node);
        }
    }
    if (currentIncludes.length > 0) {
        groups.push({ type: 'include-group', items: currentIncludes });
    }
    
    return groups;
}

// Phase 3: Compile AST to final structure
function compileAst(ast, operator) {
    const items = [];
    for (const node of ast) {
        if (node.type === 'include-group') {
            items.push(...node.items);
        } else if (node.type === 'exclude') {
            const preceding = items.splice(0);
            const source = preceding.length === 1 ? preceding[0] : { [operator]: preceding };
            const excl = node.args.length === 1 ? node.args[0] : { [operator]: node.args };
            items.push({ difference: [source, excl] });
        }
    }
    if (items.length === 1) return items[0];
    return { [operator]: items };
}

function buildWithExcludesAst(args, operator) {
    const parseTree = parseArgs(args);
    const normalized = normalizeParseTree(parseTree);
    return compileAst(normalized, operator);
}
```

### Comparison

| Aspect | Current | Intermediate AST |
|---|---|---|
| **Lines of code** | ~12 | ~40 |
| **Phases** | 1 (inline) | 3 (parse, normalize, compile) |
| **Coupling** | Tight (logic in transform) | Loose (each phase is self-contained) |
| **Debuggability** | Single pass; inspect items array | Inspect parse tree, normalized AST, final output |
| **Extensibility** | Add new logic → modify `buildWithExcludes()` | Add new transform phases without touching others |
| **Performance overhead** | None (single allocation) | ~3 allocations (parseTree, normalized, items) |
| **Readability** | Compact but stateful (splice) | Explicit state per phase, easier to follow |

### Assessment

**When the intermediate AST is BETTER:**

1. **Complex transforms planned** — if `exclude()` gains modifiers (e.g., `exclude(..., {recursive: true})`) or conditional semantics, separating parse from compile prevents the single function from ballooning.
2. **Error recovery** — a parse tree allows collecting all validation errors and reporting them at once, rather than failing on the first bad marker.
3. **Optimization passes** — a normalized AST can be analyzed (e.g., detecting `exclude(_all)`, redundant excludes) before compilation.

**When the current approach is BETTER:**

1. **Simplicity, maintainability now** — the current code is short, reads linearly, and requires no mental model of intermediate representations. Cognitive overhead is low.
2. **Performance-sensitive path** — `anyOf()` and `allOf()` are called for every query DSL build. Three allocations vs one is measurable at scale (though unlikely to matter in real workloads).
3. **Unlikely to change** — if `exclude()` semantics are stable and no new DSL features are planned, the intermediate AST adds architecture cost without payback.

**Recommendation:**

**Keep the current approach.** The problem space is narrow (two operators, one marker type), the code is already understandable, and the performance is negligible. If *in the future* you need:
- Custom validation or error recovery → revisit this decision
- Additional marker types or conditional logic → consider the three-phase approach

For now, the extra code is not justified by actual problems it solves.

---

## Part 2: `exclude()` Naming — DSL Semantics

### Problem: `exclude(b)` is Implicit

The marker function `exclude(b)` does not read semantically in context:

```javascript
anyOf('a', exclude('b'), 'c')
// Does this mean: "a OR exclude(b) OR c"? That's not it...
// Actually: "(a minus b) OR c"
// But exclude(b) by itself doesn't tell you "minus a"
```

The marker is postfix-positional: its meaning depends on what precedes it — not explicit in the call.

### Alternatives Evaluation

#### 1. `except(b)` — Minimal Change

```javascript
anyOf('a', except('b'), 'c')
```

**Pros:**
- Clean, brief
- Common English word (except = excluding)

**Cons:**
- Still postfix-positional; the semantics are not clearer than `exclude()`
- "except" is less obvious in code review (is this an error handler?)

**Assessment:** Marginal improvement. Semantic issue remains unsolved.

---

#### 2. `without(b)` — Negation Reads Better

```javascript
anyOf('a', without('b'), 'c')
```

**Pros:**
- "a without b" is immediate English
- More descriptive than `exclude()` or `except()`

**Cons:**
- Still postfix-positional; ambiguity persists
- Not substantially different from current state

**Assessment:** Moderate improvement (10–15% clearer). But still relies on position precedence.

---

#### 3. `minus(...args)` — Mathematical Parallel

```javascript
anyOf('a', minus('b'), 'c')
```

**Pros:**
- Mathematical notation: a − b is universally understood
- Concise and intent-signaling

**Cons:**
- Less familiar in programming contexts (not English-like)
- Still postfix-positional

**Assessment:** Equivalent to `except()`, trading verbal for symbolic clarity.

---

#### 4. **Binary Operator Style: Explicit Pairing** (Breaks DSL Symmetry)

```javascript
anyOf(subtract('a', 'b'), 'c')
// or
anyOf(difference('a', 'b'), 'c')
```

**Pros:**
- Explicit: `subtract('a', 'b')` reads as "a minus b"
- No postfix ambiguity

**Cons:**
- Breaks DSL symmetry: `anyOf('a', 'b')` vs `anyOf(subtract('a', 'b'))`
- Nesting becomes verbose: `anyOf(allOf('a', 'b'), subtract(allOf('x', 'y'), 'z'), 'c')`
- Harder to reason about operator precedence in deeply nested expressions

**Assessment:** Solves semantics but at high DSL cost. Not recommended.

---

#### 5. **Pipe-like Syntax: Logical Complement** (Hypothetical)

```javascript
anyOf('a', 'b', not('c'))
```

Wait — you *already have* `not()`! The function exists:

```javascript
function not(...args) {
    const excl = args.length === 1 ? args[0] : { $union: args };
    return { difference: ['_all', excl] };
}
```

So `not(c)` means `_all \ c`. But inside an `anyOf()`, this is different from `exclude()`:

```javascript
anyOf('a', 'b', not('c'))
// = ('a' OR 'b' OR (_all \ c))
// ≠ (('a' OR 'b') \ c)
```

The postfix `exclude()` is *local* exclusion (all preceding args as source), while `not()` is *global* (exclude from _all). These are semantically different operations.

**Assessment:** `not()` exists but solves a different problem (global complement). Not a replacement for `exclude()`.

---

### Root Issue: Postfix Positionals are Inherently Implicit

The core ambiguity is structural: a marker that modifies its *preceding* argument by context cannot be made fully explicit without changing the call structure. The function signature alone (`exclude(b)` with one arg) cannot convey "I modify the preceding thing."

**Possible restructurings:**

1. **Infix-to-function:** `a.minus(b)` in chained style — but breaks multivariate `anyOf()` DSL.
2. **Explicit source:** `exclude('a', 'b')` — but then which is source and which is exclusion?
3. **Wrapper object:**
   ```javascript
   anyOf('a', { subtract: 'b' }, 'c')
   // = (a - b) OR c
   ```
   More explicit but loses function-call DSL aesthetics.

4. **Accept the positional and document clearly:**
   ```javascript
   anyOf('a', exclude('b'), 'c')
   // The exclude() applies to immediately preceding args (here: 'a')
   ```
   This is the current state. The implicit rule is documented; usage patterns make it learned.

---

### Recommendation: Rename to `without()`, Document the Semantics

**Change:**

```javascript
// BEFORE
anyOf('a', exclude('b'), 'c')

// AFTER
anyOf('a', without('b'), 'c')
```

**Why:**
- `without` reads more naturally in English context ("a without b")
- Smaller cognitive friction than `exclude()`
- Postfix rule is documented but unavoidable; naming won't fully solve it — good documentation can

**How to minimize confusion:**

1. **Function name `without()`** (currently `exclude`)
2. **Updated docblock:**
   ```javascript
   /**
    * Set exclusion marker. When encountered in anyOf() or allOf(),
    * applies to **all immediately preceding arguments** in the same call.
    *
    * Examples:
    *   anyOf('a', without('b'))       => a \ b
    *   anyOf('a', 'b', without('c'))  => (a OR b) \ c
    *   allOf('a', 'b', without('c'))  => (a AND b) \ c
    *   allOf('a', without('b'), 'c')  => (a \ b) AND c
    *
    * Multiple args inside without() are joined with the parent operator.
    *   anyOf('a', without('b', 'c'))  => a \ (b OR c)
    *   allOf('a', without('b', 'c'))  => a \ (b AND c)
    */
   function without(...args) {
       assert(args.length > 0, 'without() requires at least one argument.');
       return { $exclude: args };
   }
   ```

3. **Usage guide in API docs:**
   > `without(...)` applies to all immediately preceding arguments in the same `anyOf()` or `allOf()` call.  
   > Think of it as "everything before this, without these specific streams."

---

## Summary

| Decision | Recommendation |
|---|---|
| **Intermediate AST** | Keep current approach. Three-phase design adds maintenance cost without current justification. Revisit only if marking types, conditional semantics, or validation complexity increase. |
| **`exclude()` naming** | Rename to `without()` for better readability. Document the implicit postfix-application rule clearly. Accept that the rule is positional — no naming change fully eliminates that cognitive load, but documentation + examples make learned patterns clear. |

---

## Implementation Notes

If proceeding with rename to `without()`:

1. Change function name in `src/Selector.js`
2. Update internal `isExclude()` check to `isWithout()` (or leave unchanged if checking `$exclude` symbol)
3. Update all test cases (`test/Selector.spec.js`)
4. Update docblocks and comments
5. No semantic changes to `buildWithExcludes()` logic
6. Release as minor version bump (API-visible but backward-compatible if exports both; breaking if `exclude` removed entirely)


