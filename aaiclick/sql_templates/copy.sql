CREATE TABLE {result_table}
ENGINE = MergeTree ORDER BY tuple()
AS SELECT * FROM {source_table}
