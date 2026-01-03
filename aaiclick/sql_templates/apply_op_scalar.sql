CREATE TABLE {result_table}
ENGINE = MergeTree ORDER BY tuple()
AS SELECT 1 AS aai_id, {expression} AS value
FROM {left_table} AS a, {right_table} AS b
