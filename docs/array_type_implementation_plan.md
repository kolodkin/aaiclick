# Array Type Implementation Plan

## Current Status

Array field support is implemented via `Array(T)` ClickHouse columns in the records format (list-of-dicts).

### Core Implementation

| Component                     | Status | File                             |
|-------------------------------|--------|----------------------------------|
| `ColumnInfo.array` field      | Done   | `aaiclick/data/models.py`        |
| `ColumnInfo.ch_type()` DDL    | Done   | `aaiclick/data/models.py`        |
| `parse_ch_type()` for Array   | Done   | `aaiclick/data/models.py`        |
| Records creation              | Done   | `aaiclick/data/data_context.py`  |
| Array type inference           | Done   | `aaiclick/data/data_context.py`  |
| Data extraction (dict/records) | Done   | `aaiclick/data/data_extraction.py` |
| Schema consolidation          | Done   | `aaiclick/data/models.py`        |
| `metadata()` -> `schema` prop | Done   | `aaiclick/data/object.py`        |

### Operator Support

#### Binary Operators (element-wise via `_apply_operator` / row-JOIN)

All binary operators work on array Objects via the standard row-JOIN path. These operators pair elements by row number (inner join) and **silently drop extra elements** on size mismatch.

| Operator | Python     | ClickHouse               | Array Support |
|----------|------------|--------------------------|---------------|
| `+`      | `__add__`  | `a.value + b.value`      | Supported     |
| `-`      | `__sub__`  | `a.value - b.value`      | Supported     |
| `*`      | `__mul__`  | `a.value * b.value`      | Supported     |
| `/`      | `__truediv__` | `a.value / b.value`   | Supported     |
| `//`     | `__floordiv__` | `intDiv(a.value, b.value)` | Supported |
| `%`      | `__mod__`  | `a.value % b.value`      | Supported     |
| `**`     | `__pow__`  | `power(a.value, b.value)` | Supported    |
| `==`     | `__eq__`   | `a.value = b.value`      | Supported     |
| `!=`     | `__ne__`   | `a.value != b.value`     | Supported     |
| `<`      | `__lt__`   | `a.value < b.value`      | Supported     |
| `<=`     | `__le__`   | `a.value <= b.value`     | Supported     |
| `>`      | `__gt__`   | `a.value > b.value`      | Supported     |
| `>=`     | `__ge__`   | `a.value >= b.value`     | Supported     |
| `&`      | `__and__`  | `bitAnd(a.value, b.value)` | Supported   |
| `\|`     | `__or__`   | `bitOr(a.value, b.value)` | Supported    |
| `^`      | `__xor__`  | `bitXor(a.value, b.value)` | Supported   |

All binary operators also support:
- Scalar broadcast: `obj + 5`, `5 + obj` (reverse operators)
- Array x Array: `obj_a + obj_b`

#### arrayMap Operators (strict size validation)

`array_map(other, operator)` collects both operands into ClickHouse arrays, applies `arrayMap()`, and expands back. **Raises DB::Exception on size mismatch** (unlike binary operators which silently drop).

| Operator | arrayMap Expression         | Tested |
|----------|-----------------------------|--------|
| `+`      | `x + y`                     | Yes    |
| `-`      | `x - y`                     | Yes    |
| `*`      | `x * y`                     | Yes    |
| `/`      | `intDiv(x, y)`              | Yes    |
| `%`      | `x % y`                     | Yes    |
| `**`     | `power(x, y)`               | Yes    |
| `==`     | `toUInt8(x = y)`            | Yes    |
| `!=`     | `toUInt8(x != y)`           | Yes    |
| `<`      | `toUInt8(x < y)`            | Yes    |
| `<=`     | `toUInt8(x <= y)`           | Yes    |
| `>`      | `toUInt8(x > y)`            | Yes    |
| `>=`     | `toUInt8(x >= y)`           | Yes    |
| `&`      | `bitAnd(x, y)`              | Yes    |
| `\|`     | `bitOr(x, y)`              | Yes    |
| `^`      | `bitXor(x, y)`              | Yes    |

#### Aggregation Operators

Reduce array Object to scalar Object.

| Operator | ClickHouse Function | Array Support |
|----------|---------------------|---------------|
| `min()`  | `min(value)`        | Supported     |
| `max()`  | `max(value)`        | Supported     |
| `sum()`  | `sum(value)`        | Supported     |
| `mean()` | `avg(value)`        | Supported     |
| `std()`  | `stddevPop(value)`  | Supported     |
| `var()`  | `varPop(value)`     | Supported     |

#### Other Operations

| Operation              | Array Support | Notes                                        |
|------------------------|---------------|----------------------------------------------|
| `copy()`               | Supported     | Materializes to new table                    |
| `concat(*args)`        | Supported     | Preserves Snowflake ID ordering              |
| `insert(*args)`        | Supported     | Validates fieldtype is array                 |
| `unique()`             | Supported     | Deduplicates values                          |
| `match(pattern)`       | Supported     | Regex match on string arrays                 |
| `view()`               | Supported     | WHERE, LIMIT, OFFSET, ORDER BY               |
| `group_by().agg()`     | Supported     | For scalar/array Objects with `value` column |
| `data(orient=...)`     | Supported     | ORIENT_DICT, ORIENT_RECORDS                  |

### Not Supported

| Feature                       | Reason                                          |
|-------------------------------|------------------------------------------------|
| GROUP BY on Array(T) columns  | No `groupArray()` / `groupUniqArray()` support  |
| Array functions               | `arrayLength()`, `arraySlice()`, etc. not exposed |
| Nested arrays                 | `Array(Array(T))` not supported                 |
| Array column binary operators | Direct ops on `Array(T)` columns not routed     |

## Test Coverage

| Test File                            | Covers                                    |
|--------------------------------------|-------------------------------------------|
| `test_type_array_field.py`           | Records creation, type inference, schema  |
| `test_array_map.py`                  | All arrayMap operators, broadcasts, errors |
| `test_insert_parametrized.py`        | Array insertion (array+array, array+scalar, array+list) |
| `test_concat_parametrized.py`        | Array concatenation                       |
| `test_copy_parametrized.py`          | Copying array objects                     |
| `test_creation_parametrized.py`      | Creation scenarios                        |
| `test_operators_parametrized.py`     | Binary operators on arrays                |
| `test_schema.py`                     | Schema property for all types             |
