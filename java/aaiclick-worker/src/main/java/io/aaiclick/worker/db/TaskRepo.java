package io.aaiclick.worker.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/** tasks-table operations: claiming and the run lifecycle.
 *
 * The claim SQL is PgDbHandler.claim_next_task() + DEPENDENCY_WHERE from
 * aaiclick/orchestration/execution/{pg_handler,db_handler}.py ported
 * verbatim, with two Java-worker capability predicates appended:
 * entry_type = 'shell' AND image_source IS NULL.
 */
public class TaskRepo {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String DEPENDENCY_WHERE = """
        AND NOT EXISTS (
            SELECT 1 FROM dependencies d
            JOIN tasks prev ON d.previous_id = prev.id
            WHERE d.next_id = t.id
            AND d.next_type = 'task'
            AND d.previous_type = 'task'
            AND prev.status != 'COMPLETED'
        )
        AND NOT EXISTS (
            SELECT 1 FROM dependencies d
            JOIN tasks prev ON prev.group_id = d.previous_id
            WHERE d.next_id = t.id
            AND d.next_type = 'task'
            AND d.previous_type = 'group'
            AND prev.status != 'COMPLETED'
        )
        AND NOT EXISTS (
            SELECT 1 FROM dependencies d
            JOIN tasks prev ON d.previous_id = prev.id
            WHERE d.next_id = t.group_id
            AND d.next_type = 'group'
            AND d.previous_type = 'task'
            AND prev.status != 'COMPLETED'
            AND t.group_id IS NOT NULL
        )
        AND NOT EXISTS (
            SELECT 1 FROM dependencies d
            JOIN tasks prev ON prev.group_id = d.previous_id
            WHERE d.next_id = t.group_id
            AND d.next_type = 'group'
            AND d.previous_type = 'group'
            AND prev.status != 'COMPLETED'
            AND t.group_id IS NOT NULL
        )
        """;

    private static final String CLAIM_SQL = """
        WITH claimed_task AS (
            UPDATE tasks
            SET status = 'RUNNING', execution_worker_id = ?, claimed_at = ?
            WHERE id = (
                SELECT t.id FROM tasks t
                JOIN jobs j ON t.job_id = j.id
                WHERE t.status = 'PENDING'
                AND (t.retry_after IS NULL OR t.retry_after <= ?)
                AND j.status NOT IN ('CANCELLED', 'FAILED')
                AND t.entry_type = 'shell'
                AND t.image_source IS NULL
                """ + DEPENDENCY_WHERE + """
                ORDER BY j.started_at ASC NULLS LAST, t.id ASC
                LIMIT 1
                FOR UPDATE OF t SKIP LOCKED
            )
            RETURNING id, job_id, name, command, command_env, run_epoch
        ),
        updated_job AS (
            UPDATE jobs
            SET started_at = COALESCE(started_at, ?),
                status = CASE WHEN started_at IS NULL THEN 'RUNNING' ELSE status END
            WHERE id = (SELECT job_id FROM claimed_task)
            RETURNING id
        )
        SELECT * FROM claimed_task
        """;

    protected final Db db;

    public TaskRepo(Db db) {
        this.db = db;
    }

    public ClaimedTask claimNext(long workerId) throws SQLException {
        try (Connection conn = db.connect(); PreparedStatement stmt = conn.prepareStatement(CLAIM_SQL)) {
            Timestamp now = Timestamp.from(Instant.now());
            stmt.setLong(1, workerId);
            stmt.setTimestamp(2, now);
            stmt.setTimestamp(3, now);
            stmt.setTimestamp(4, now);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return new ClaimedTask(
                    rs.getLong("id"),
                    rs.getLong("job_id"),
                    rs.getString("name"),
                    parseJson(rs.getString("command"), new TypeReference<List<String>>() {}),
                    parseJson(rs.getString("command_env"), new TypeReference<Map<String, String>>() {}),
                    rs.getLong("run_epoch"));
            }
        }
    }

    private static <T> T parseJson(String json, TypeReference<T> type) {
        if (json == null) {
            return null;
        }
        try {
            return MAPPER.readValue(json, type);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Malformed JSON column: " + json, e);
        }
    }
}
