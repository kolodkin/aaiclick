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

### Validating Complex SQL

Use chdb to validate ClickHouse SQL syntax before running it on a real server:

```python
import chdb

# Test complex SQL with subqueries and joins
sql = """
SELECT a.id, a.value + b.value AS total
FROM (SELECT number as id, number * 10 as value FROM numbers(5)) AS a
INNER JOIN (SELECT number as id, number * 2 as value FROM numbers(5)) AS b
ON a.id = b.id
"""

try:
    result = chdb.query(sql)
    print("✓ SQL is valid")
    print(result)
except Exception as e:
    print(f"✗ SQL error: {e}")
```

### Using Sessions for Stateful Operations

```python
import chdb

# Create a session for operations requiring tables
session = chdb.Session()

# Create a table
session.query("CREATE TABLE users (id UInt64, name String) ENGINE = Memory")

# Insert data
session.query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")

# Query it
result = session.query("SELECT * FROM users WHERE id > 1")
print(result)

# Clean up
session.query("DROP TABLE users")
```

### Testing Subquery Aliasing

```python
import chdb

# Test that subqueries require aliases
sql_with_alias = """
SELECT id, value FROM (SELECT number as id, number * 10 as value FROM numbers(10)) AS subquery
WHERE value > 50
"""

sql_without_alias = """
SELECT id, value FROM (SELECT number as id, number * 10 as value FROM numbers(10))
WHERE value > 50
"""

try:
    chdb.query(sql_with_alias)
    print("✓ Query with alias works")
except Exception as e:
    print(f"✗ With alias failed: {e}")

try:
    chdb.query(sql_without_alias)
    print("✓ Query without alias works")
except Exception as e:
    print(f"✗ Without alias failed (expected): {e}")
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

## Common Validation Patterns

### Check SQL Clause Order

```python
import chdb

# Verify ORDER BY comes before LIMIT
valid_sql = "SELECT * FROM numbers(100) WHERE number > 50 ORDER BY number LIMIT 10"
invalid_sql = "SELECT * FROM numbers(100) WHERE number > 50 LIMIT 10 ORDER BY number"

try:
    chdb.query(valid_sql)
    print("✓ Correct clause order works")
except Exception as e:
    print(f"✗ Valid SQL failed: {e}")

try:
    chdb.query(invalid_sql)
    print("✗ Invalid clause order shouldn't work")
except Exception as e:
    print(f"✓ Invalid SQL correctly rejected: {e}")
```

### Test UNION ALL

```python
import chdb

# Validate UNION ALL syntax
sql = """
SELECT number as id, 'group1' as source FROM numbers(5)
UNION ALL
SELECT number + 100 as id, 'group2' as source FROM numbers(5)
"""

result = chdb.query(sql)
print(result)
```

## Documentation

For more details, see the official chdb documentation:
- **GitHub**: https://github.com/chdb-io/chdb
- **PyPI**: https://pypi.org/project/chdb/
- **Documentation**: https://doc.chdb.io/

## Tips

1. **Always alias subqueries** in FROM clauses to avoid errors
2. **Use sessions** when you need to create tables or maintain state
3. **Test incrementally** - start with simple queries and build up complexity
4. **Leverage numbers()** table function for generating test data without INSERT statements
