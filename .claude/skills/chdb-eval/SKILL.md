# chdb-eval: Evaluating ClickHouse SQL Without a Server

This skill explains how to use **chdb** (ClickHouse embedded in Python) to evaluate and validate ClickHouse SQL syntax when you don't have access to a running ClickHouse server.

## What is chdb?

chdb is an embedded ClickHouse engine for Python that allows you to run ClickHouse SQL queries without needing a separate ClickHouse server. It's perfect for:
- Development environments without ClickHouse installed
- Quick SQL syntax validation
- Testing query logic offline
- CI/CD environments where you want lightweight testing

## Installation

```bash
pip install chdb
```

## Basic Usage

### Simple Query Execution

```python
import chdb

# Execute a simple query
result = chdb.query("SELECT 1 + 1")
print(result)
```

### Validating SQL Syntax

Use chdb to validate ClickHouse SQL syntax before running it on a real server:

```python
import chdb

# Test complex SQL with subqueries
sql = """
SELECT a.rn as aai_id, a.value + b.value AS value
FROM (SELECT row_number() OVER (ORDER BY number) as rn, number as value FROM numbers(5)) AS a
INNER JOIN (SELECT row_number() OVER (ORDER BY number) as rn, number * 2 as value FROM numbers(5)) AS b
ON a.rn = b.rn
"""

try:
    result = chdb.query(sql)
    print("✓ SQL is valid")
    print(result)
except Exception as e:
    print(f"✗ SQL error: {e}")
```

### Testing VIEW Constraints and Subqueries

When working with aaiclick's View constraints (WHERE, LIMIT, OFFSET, ORDER BY), you can validate the generated SQL:

```python
import chdb

# Simulate a view with constraints
base_query = "SELECT number as aai_id, number * 10 as value FROM numbers(100)"
where_clause = "value > 200"
order_by = "aai_id"
limit = 10
offset = 5

# Build the subquery like aaiclick does
subquery = f"({base_query} WHERE {where_clause} ORDER BY {order_by} LIMIT {limit} OFFSET {offset})"

# Test in a larger query
full_query = f"""
SELECT aai_id, value FROM {subquery} AS s0
UNION ALL
SELECT aai_id, value FROM (SELECT number as aai_id, number * 5 as value FROM numbers(10)) AS s1
"""

result = chdb.query(full_query)
print(result)
```

### Testing UNION ALL with Subqueries

Validate concat operations that use UNION ALL with subquery aliasing:

```python
import chdb

# Test the pattern used in concat_objects_db
sources = [
    "(SELECT number as aai_id, number as value FROM numbers(3))",
    "(SELECT number + 10 as aai_id, number + 100 as value FROM numbers(3))",
    "simple_table"  # This would need to exist, so use a generator instead
]

union_parts = []
for i, source in enumerate(sources[:2]):  # Skip the table for now
    if source.startswith('('):
        union_parts.append(f"SELECT aai_id, value FROM {source} AS s{i}")
    else:
        union_parts.append(f"SELECT aai_id, value FROM {source}")

query = ' UNION ALL '.join(union_parts)
result = chdb.query(query)
print(result)
```

### Creating Temporary Tables for Testing

```python
import chdb

# Create a session for stateful operations
session = chdb.Session()

# Create a table
session.query("CREATE TABLE test_table (aai_id UInt64, value Int64) ENGINE = Memory")

# Insert data
session.query("INSERT INTO test_table VALUES (1, 10), (2, 20), (3, 30)")

# Query it
result = session.query("SELECT * FROM test_table WHERE value > 15")
print(result)

# Clean up
session.query("DROP TABLE test_table")
```

### Testing Operator Expressions

Validate the operator expressions used in aaiclick:

```python
import chdb

# Test arithmetic operators
operators = {
    "+": "a.value + b.value",
    "-": "a.value - b.value",
    "*": "a.value * b.value",
    "/": "a.value / b.value",
    "//": "intDiv(a.value, b.value)",
    "%": "a.value % b.value",
    "**": "power(a.value, b.value)",
}

for op_name, expression in operators.items():
    query = f"""
    SELECT {expression} as result
    FROM (SELECT 10 as value) AS a, (SELECT 3 as value) AS b
    """
    try:
        result = chdb.query(query)
        print(f"✓ {op_name}: {result.strip()}")
    except Exception as e:
        print(f"✗ {op_name} failed: {e}")
```

## When to Use chdb

**Use chdb when:**
- Developing on a machine without ClickHouse server
- Testing SQL syntax quickly without database setup
- Validating query structure before pushing to CI/CD
- Writing documentation examples
- Debugging complex subqueries

**Don't use chdb when:**
- Testing against actual production data
- Validating system.columns metadata queries (chdb has limited system tables)
- Testing distributed table operations
- Performance testing (use real ClickHouse server)

## Tips for aaiclick Development

1. **Test subquery aliasing**: Always test that subqueries in FROM clauses have proper aliases
2. **Validate ORDER BY placement**: Ensure ORDER BY comes before LIMIT/OFFSET
3. **Test UNION ALL**: Validate multi-source concatenation queries
4. **Check operator expressions**: Test all 14 operators with sample data

## Documentation

For more details, see the official chdb documentation:
- **GitHub**: https://github.com/chdb-io/chdb
- **PyPI**: https://pypi.org/project/chdb/
- **Documentation**: https://doc.chdb.io/

## Example: Testing aaiclick View SQL Generation

```python
import chdb

# Simulate what Object._build_select() generates
def build_select(table, where=None, order_by=None, limit=None, offset=None, columns="*"):
    query = f"SELECT {columns} FROM {table}"
    if where:
        query += f" WHERE {where}"
    if order_by:
        query += f" ORDER BY {order_by}"
    if limit is not None:
        query += f" LIMIT {limit}"
    if offset is not None:
        query += f" OFFSET {offset}"
    return query

# Test various constraint combinations
test_cases = [
    {"where": "value > 5", "limit": 10},
    {"where": "value > 5", "order_by": "value", "limit": 10},
    {"limit": 5, "offset": 2},
    {"where": "value BETWEEN 10 AND 50", "order_by": "value DESC", "limit": 20, "offset": 5},
]

base_table = "(SELECT number as aai_id, number * 10 as value FROM numbers(100))"

for i, kwargs in enumerate(test_cases, 1):
    sql = build_select(base_table, **kwargs)
    try:
        result = chdb.query(sql)
        print(f"✓ Test {i} passed: {kwargs}")
        print(f"  Result rows: {len(result.strip().split(chr(10)))}")
    except Exception as e:
        print(f"✗ Test {i} failed: {kwargs}")
        print(f"  Error: {e}")
```

## Integration with Claude Code

When Claude is working on aaiclick SQL generation:

1. **Ask Claude to validate with chdb**: "Can you test this SQL with chdb to make sure it's valid?"
2. **Test before pushing**: Validate complex queries locally before committing
3. **Debug syntax errors**: Use chdb to quickly identify SQL syntax issues

This approach helps catch SQL errors early without needing a full ClickHouse setup.
