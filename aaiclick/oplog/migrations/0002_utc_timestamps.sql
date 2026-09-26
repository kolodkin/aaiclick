-- Pin internal timestamp columns to UTC. Without an explicit zone they use
-- the server's zone (the host's, under chdb), while aaiclick writes naive
-- UTC. Changing only the zone is a metadata edit: stored values are not
-- rewritten, and ClickHouse allows it on sort-key columns.
--
-- schema_migrations is created before any migration runs, so installs that
-- predate the UTC SCHEMA_MIGRATIONS_DDL are fixed here as well.

ALTER TABLE operation_log MODIFY COLUMN created_at DateTime64(3, 'UTC');

ALTER TABLE task_logs MODIFY COLUMN created_at DateTime64(3, 'UTC');

ALTER TABLE schema_migrations MODIFY COLUMN applied_at DateTime64(3, 'UTC');
