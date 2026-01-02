CREATE TABLE {result_table}
ENGINE = MergeTree ORDER BY tuple()
AS SELECT {operator}(a.value, b.value) AS value
FROM {left_table} AS a, {right_table} AS b
