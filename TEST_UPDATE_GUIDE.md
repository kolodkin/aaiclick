# Test Update Guide

This document describes how to update existing tests to use the new Context-based API.

## Summary of Changes

The factory functions `create_object()` and `create_object_from_value()` are now **internal** and should only be called via `Context`. All tests should use the `ctx` fixture.

## Pattern for Updating Tests

### Before (Old Pattern):
```python
from aaiclick import create_object_from_value

async def test_example():
    obj = await create_object_from_value([1, 2, 3])
    data = await obj.data()
    assert data == [1, 2, 3]
    await obj.delete_table()
```

### After (New Pattern):
```python
async def test_example(ctx):
    obj = await ctx.create_object_from_value([1, 2, 3])
    data = await obj.data()
    assert data == [1, 2, 3]
    # No delete_table() needed - context handles cleanup
```

## Step-by-Step Update Instructions

1. **Remove factory imports**:
   - Remove: `from aaiclick import create_object_from_value`
   - Remove: `from aaiclick import create_object`

2. **Add `ctx` parameter to test functions**:
   - Change: `async def test_foo():`
   - To: `async def test_foo(ctx):`

3. **Update factory calls**:
   - Change: `obj = await create_object_from_value([1, 2, 3])`
   - To: `obj = await ctx.create_object_from_value([1, 2, 3])`

4. **Remove manual cleanup for context-created objects**:
   - Objects created via `ctx` are automatically cleaned up
   - Remove calls like: `await obj.delete_table()`
   - **KEEP** cleanup for operator results: `result = await (a + b)` still needs `await result.delete_table()`

## Example: Operator Test

### Before:
```python
async def test_add():
    a = await create_object_from_value([1, 2, 3])
    b = await create_object_from_value([4, 5, 6])
    result = await (a + b)

    data = await result.data()
    assert data == [5, 7, 9]

    await a.delete_table()
    await b.delete_table()
    await result.delete_table()
```

### After:
```python
async def test_add(ctx):
    a = await ctx.create_object_from_value([1, 2, 3])
    b = await ctx.create_object_from_value([4, 5, 6])
    result = await (a + b)

    data = await result.data()
    assert data == [5, 7, 9]

    # a and b cleaned by context automatically
    await result.delete_table()  # Still needed for operator result
```

## Files That Need Updating

- [ ] tests/test_type_int.py
- [ ] tests/test_type_float.py
- [ ] tests/test_type_bool.py
- [ ] tests/test_type_str.py
- [ ] tests/test_type_dict.py
- [ ] tests/test_type_mixed.py
- [ ] tests/test_operators_parametrized.py
- [ ] tests/test_operator_large.py
- [ ] tests/test_examples.py
- [x] tests/test_context.py (already updated)

## Automated Update Script

You can use this sed command pattern to help automate updates:

```bash
# Add ctx parameter to test functions
sed -i 's/async def test_\(.*\)():/async def test_\1(ctx):/' test_file.py

# Update create_object_from_value calls
sed -i 's/await create_object_from_value(/await ctx.create_object_from_value(/g' test_file.py

# Update create_object calls
sed -i 's/await create_object(/await ctx.create_object(/g' test_file.py
```

**Note**: Manual review is still needed to determine which `delete_table()` calls to remove.
