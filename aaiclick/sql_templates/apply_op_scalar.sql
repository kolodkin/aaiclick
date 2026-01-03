CREATE TABLE {result_table}
ENGINE = MergeTree ORDER BY tuple() {ttl_clause}
AS SELECT 1 AS aai_id, {expression} AS value, now() AS created_at
FROM {left_table} AS a, {right_table} AS b
