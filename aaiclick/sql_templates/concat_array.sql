CREATE TABLE {result_table}
ENGINE = MergeTree ORDER BY aai_id {ttl_clause}
AS
SELECT row_number() OVER (ORDER BY t, aai_id) as aai_id, value, now() AS created_at
FROM (
    SELECT 1 as t, aai_id, value FROM {left_table}
    UNION ALL
    SELECT 2 as t, aai_id, value FROM {right_table}
)
