CREATE TABLE {result_table}
ENGINE = MergeTree ORDER BY tuple()
AS
SELECT row_number() OVER (ORDER BY source_order, original_aai_id) as aai_id, value
FROM (
    SELECT 1 as source_order, aai_id as original_aai_id, value FROM {left_table}
    UNION ALL
    SELECT 2 as source_order, aai_id as original_aai_id, value FROM {right_table}
)
