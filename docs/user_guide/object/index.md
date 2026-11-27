Object
---

# Overview

An `Object` wraps one ClickHouse table holding a scalar, an array, or a dict.
Arithmetic, comparison, and bitwise operators work element-wise and write results
to new tables. Structure is immutable: `insert()` appends data but never changes
the schema.

See [ObjectContext](../object_context.md) for lifecycle, schemas, and deployment modes, and
[Ingest](../ingest.md) for loading, combining and joining data.

# API Quick Reference

| API                                             | Category         | Description                                                            | Section                                                              |
|-------------------------------------------------|------------------|------------------------------------------------------------------------|----------------------------------------------------------------------|
| `+`, `-`, `*`, `/`, `//`, `%`, `**`             | Arithmetic       | Element-wise arithmetic                                                | [Operator Support](operators.md#operator-support)                                |
| `==`, `!=`, `<`, `<=`, `>`, `>=`                | Comparison       | Element-wise comparison                                                | [Operator Support](operators.md#operator-support)                                |
| `&`, `\|`, `^`                                  | Bitwise          | Bitwise AND / OR / XOR                                                 | [Operator Support](operators.md#operator-support)                                |
| `.min()`, `.max()`, `.sum()`, `.mean()`         | Aggregation      | Reduce array to scalar                                                 | [Aggregation Operators](operators.md#aggregation-operators)                      |
| `.std()`, `.var()`, `.count()`, `.quantile(q)`  | Aggregation      | Statistical reduction                                                  | [Aggregation Operators](operators.md#aggregation-operators)                      |
| `.count_if(condition)`                          | Aggregation      | Count rows matching condition(s)                                       | [Aggregation Operators](operators.md#aggregation-operators)                      |
| `.unique()`                                     | Unique           | Deduplicate values via GROUP BY                                        | [Unique](operators.md#unique)                                                    |
| `.nunique()`                                    | Unique           | Count distinct values (fused)                                          | [Unique](operators.md#unique)                                                    |
| `.isin(other)`                                  | Membership       | IN subquery test (returns UInt8 mask)                                  | [Membership Operator](operators.md#membership-operator-isin)                     |
| `.match(p)`, `.like(p)`, `.ilike(p)`            | String / Regex   | Pattern matching (returns UInt8 mask)                                  | [String/Regex Operators](operators.md#stringregex-operators)                     |
| `.extract(p)`, `.replace(p, r)`                 | String / Regex   | Capture group extraction, regex replace                                | [String/Regex Operators](operators.md#stringregex-operators)                     |
| `.year()`, `.month()`, `.day_of_week()`         | Unary Transforms | Date/time extraction → new Object                                      | [Unary Transform Operators](operators.md#unary-transform-operators)              |
| `.lower()`, `.upper()`, `.length()`, `.trim()`  | Unary Transforms | String transforms → new Object                                         | [Unary Transform Operators](operators.md#unary-transform-operators)              |
| `.abs()`, `.log2()`, `.sqrt()`                  | Unary Transforms | Math functions → new Object                                            | [Unary Transform Operators](operators.md#unary-transform-operators)              |
| `.group_by(keys).sum(col)` etc.                 | Group By         | Aggregation with GROUP BY + optional HAVING                            | [Group By Operations](operators.md#group-by-operations)                          |
| `.group_by(keys).agg({col: spec})`              | Group By         | Multiple aggregations (multi-agg per column)                           | [Group By Operations](operators.md#group-by-operations)                          |
| `.group_by(keys).any(col)`                      | Group By         | Arbitrary non-NULL value per group                                     | [Group By Operations](operators.md#group-by-operations)                          |
| `.group_by(keys).group_array_distinct(col)`     | Group By         | Distinct values → Array per group                                      | [Group By Operations](operators.md#group-by-operations)                          |
| `.view(where, limit, offset, order_by)`         | Views            | Read-only View with optional filters                                   | [Views](views.md#views)                                                      |
| `.select_sql(columns, order_by, limit, offset)` | Views            | The SELECT text the object reads with; Views include their constraints | [Views](views.md#views)                                                      |
| `.where(cond)` / `.or_where(cond)`              | Views            | Fluent WHERE chaining (AND / OR)                                       | [Chained WHERE Clauses](views.md#chained-where-clauses)                      |
| `obj[key]` / `obj[[keys]]`                      | Views            | Select column(s) from dict Object → View                               | [Column Selection](views.md#column-selection)                                |
| `.with_columns({name: Computed(type, expr)})`   | Views            | Add SQL expression columns → View                                      | [Computed Column Expansion](views.md#computed-column-expansion-with_columns) |
| `literal(value, ch_type)`                       | Helpers          | Constant column (`Computed` with SQL literal)                          | [Computed Column Expansion](views.md#computed-column-expansion-with_columns) |
| `.with_year(col)`, `.with_month(col)` …         | Views            | Named shortcuts for common computed columns                            | [Domain Helpers](views.md#domain-helpers)                                    |
| `.with_split_by_char(col, sep)`                 | Views            | Split String column by separator → Array                               | [Domain Helpers](views.md#domain-helpers)                                    |
| `.with_isin(col, other)`                        | Views            | IN subquery computed column → UInt8                                    | [Domain Helpers](views.md#domain-helpers)                                    |
| `.rename({old: new})`                           | Views            | Alias column names in a View                                           | [Column Renaming](views.md#column-renaming-rename)                           |
| `.explode(*columns, left=False)`                | Views            | Flatten Array column(s) into rows → View                               | [Explode](views.md#explode)                                                  |
| `.copy()`                                       | Copy             | Materialize Object / View to new Object                                | [copy()](views.md#the-copy-method)                                           |
| `.insert(*sources)`                             | Ingest           | Insert data from Objects / scalars / lists                             | [insert()](../ingest.md#insert)                                         |
| `.insert_from_url(url)`                         | Ingest           | Insert rows from a remote URL                                          | [insert_from_url()](../ingest.md#insert_from_url)                       |
| `.concat(*sources)` / `concat(a, b, …)`         | Ingest           | Concatenate sources into a new Object                                  | [concat()](../ingest.md#concat)                                         |
| `.join(other, on, how)`                         | Join             | Join two Objects on key columns → dict Object                          | [join()](../ingest.md#join)                                             |
| `.data(orient=…)`                               | Data Retrieval   | Fetch results to Python (scalar / list / dict)                         | [data()](data_retrieval.md#data)                                                      |
| `.markdown(truncate=…)`                         | Data Retrieval   | Render data as markdown table                                          | [markdown()](data_retrieval.md#markdown)                                              |
| `.execute(order_by, limit, offset)`             | Data Retrieval   | Run full query, discard rows → `QueryStats`                            | [execute()](data_retrieval.md#execute)                                                |
| `await .stats()`                                | Data Retrieval   | `QueryStats` of the query that made this Object                        | [stats()](data_retrieval.md#stats)                                                    |
| `.export(path, overwrite, format, ch_settings)` | Export           | Stream data to a file or bucket URL (extension → format)               | [export()](export.md#export)                                                  |
| `open_object_from_url(url)`                     | Blob Storage     | Read-only Object over a bucket file, queried in place                  | [Blob Storage](blob_storage.md#blob-storage)                                        |

# Operation Provenance (Oplog)

Every Object operation is recorded for lineage tracing — see [Lineage via MCP](../lineage.md).
