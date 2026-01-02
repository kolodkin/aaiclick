CREATE TABLE {result_table}
ENGINE = MergeTree ORDER BY tuple()
AS
SELECT a.rn as aai_id, {operator}(a.value, b.value) AS value
FROM (SELECT row_number() OVER (ORDER BY aai_id) as rn, value FROM {left_table}) AS a
INNER JOIN (SELECT row_number() OVER (ORDER BY aai_id) as rn, value FROM {right_table}) AS b
ON a.rn = b.rn
