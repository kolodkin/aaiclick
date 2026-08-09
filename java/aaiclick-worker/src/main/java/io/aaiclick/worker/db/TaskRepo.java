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
 * The claim query is the shared SQL contract with the Python worker —
 * sql/claim_next_task.sql from aaiclick/orchestration/execution, embedded
 * as the /aaiclick-sql classpath resource at build time. Worker capability
 * differences are bound values (entry_types, allow_image_tasks), never
 * query edits; this worker claims shell-only, host-subprocess tasks.
 */
public class TaskRepo {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final NamedParamSql CLAIM_SQL =
        NamedParamSql.fromResource("/aaiclick-sql/claim_next_task.sql");

    protected final Db db;

    public TaskRepo(Db db) {
        this.db = db;
    }

    public ClaimedTask claimNext(long workerId) throws SQLException {
        try (Connection conn = db.connect();
             PreparedStatement stmt = conn.prepareStatement(CLAIM_SQL.jdbcSql())) {
            Timestamp now = Timestamp.from(Instant.now());
            List<String> order = CLAIM_SQL.paramOrder();
            for (int i = 0; i < order.size(); i++) {
                switch (order.get(i)) {
                    case "execution_worker_id" -> stmt.setLong(i + 1, workerId);
                    case "now" -> stmt.setTimestamp(i + 1, now);
                    case "entry_types" ->
                        stmt.setArray(i + 1, conn.createArrayOf("varchar", new String[] {"shell"}));
                    case "allow_image_tasks" -> stmt.setBoolean(i + 1, false);
                    default -> throw new IllegalStateException(
                        "Unknown parameter in shared claim SQL: " + order.get(i));
                }
            }
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

    public void startRun(long taskId, long runId) throws SQLException {
        try (Connection conn = db.connect()) {
            conn.setAutoCommit(false);
            try (PreparedStatement select = conn.prepareStatement(
                     "SELECT run_ids::text, run_statuses::text FROM tasks WHERE id = ? FOR UPDATE")) {
                select.setLong(1, taskId);
                try (ResultSet rs = select.executeQuery()) {
                    if (!rs.next()) {
                        conn.rollback();
                        return;
                    }
                    List<Long> runIds = new java.util.ArrayList<>(
                        parseJson(rs.getString(1), new TypeReference<List<Long>>() {}));
                    List<String> runStatuses = new java.util.ArrayList<>(
                        parseJson(rs.getString(2), new TypeReference<List<String>>() {}));
                    runIds.add(runId);
                    runStatuses.add("RUNNING");
                    try (PreparedStatement update = conn.prepareStatement(
                             "UPDATE tasks SET started_at = ?, run_ids = ?::json, run_statuses = ?::json"
                             + " WHERE id = ?")) {
                        update.setTimestamp(1, Timestamp.from(Instant.now()));
                        update.setString(2, MAPPER.writeValueAsString(runIds));
                        update.setString(3, MAPPER.writeValueAsString(runStatuses));
                        update.setLong(4, taskId);
                        update.executeUpdate();
                    }
                }
            } catch (JsonProcessingException e) {
                conn.rollback();
                throw new IllegalStateException("Failed to serialize run arrays", e);
            }
            conn.commit();
        }
    }

    public boolean complete(long taskId, long expectedEpoch) throws SQLException {
        return finishRun(taskId, expectedEpoch, "COMPLETED", null);
    }

    public boolean failPendingCleanup(long taskId, long expectedEpoch, String error) throws SQLException {
        return finishRun(taskId, expectedEpoch, "PENDING_CLEANUP", error);
    }

    /** Epoch-fenced terminal write; the last run_statuses entry mirrors the
     *  task outcome (COMPLETED, or FAILED for the PENDING_CLEANUP path). */
    private boolean finishRun(long taskId, long expectedEpoch, String status, String error) throws SQLException {
        String runStatus = status.equals("COMPLETED") ? "COMPLETED" : "FAILED";
        try (Connection conn = db.connect()) {
            conn.setAutoCommit(false);
            try (PreparedStatement select = conn.prepareStatement(
                     "SELECT run_statuses::text FROM tasks WHERE id = ? AND run_epoch = ? FOR UPDATE")) {
                select.setLong(1, taskId);
                select.setLong(2, expectedEpoch);
                try (ResultSet rs = select.executeQuery()) {
                    if (!rs.next()) {
                        conn.rollback();
                        return false;
                    }
                    List<String> runStatuses = new java.util.ArrayList<>(
                        parseJson(rs.getString(1), new TypeReference<List<String>>() {}));
                    if (!runStatuses.isEmpty()) {
                        runStatuses.set(runStatuses.size() - 1, runStatus);
                    }
                    try (PreparedStatement update = conn.prepareStatement(
                             "UPDATE tasks SET status = ?, completed_at = ?, error = ?, run_statuses = ?::json"
                             + " WHERE id = ? AND run_epoch = ?")) {
                        update.setString(1, status);
                        update.setTimestamp(2, Timestamp.from(Instant.now()));
                        update.setString(3, error);
                        update.setString(4, MAPPER.writeValueAsString(runStatuses));
                        update.setLong(5, taskId);
                        update.setLong(6, expectedEpoch);
                        boolean updated = update.executeUpdate() > 0;
                        conn.commit();
                        return updated;
                    }
                }
            } catch (JsonProcessingException e) {
                conn.rollback();
                throw new IllegalStateException("Failed to serialize run_statuses", e);
            }
        }
    }

    public boolean isRunAborted(long taskId, long expectedEpoch) throws SQLException {
        try (Connection conn = db.connect();
             PreparedStatement stmt = conn.prepareStatement(
                 "SELECT status, run_epoch FROM tasks WHERE id = ?")) {
            stmt.setLong(1, taskId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return false;
                }
                return "CANCELLED".equals(rs.getString(1)) || rs.getLong(2) != expectedEpoch;
            }
        }
    }

    public void tryCompleteJob(long jobId) throws SQLException {
        try (Connection conn = db.connect();
             PreparedStatement stmt = conn.prepareStatement("""
                 UPDATE jobs SET
                     status = CASE WHEN EXISTS (
                         SELECT 1 FROM tasks WHERE job_id = ?
                         AND status IN ('FAILED', 'UPSTREAM_FAILED')
                     ) THEN 'FAILED' ELSE 'COMPLETED' END,
                     error = CASE WHEN EXISTS (
                         SELECT 1 FROM tasks WHERE job_id = ?
                         AND status IN ('FAILED', 'UPSTREAM_FAILED')
                     ) THEN 'One or more tasks failed' ELSE error END,
                     completed_at = ?
                 WHERE id = ?
                 AND status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')
                 AND NOT EXISTS (
                     SELECT 1 FROM tasks WHERE job_id = ?
                     AND status IN ('PENDING', 'CLAIMED', 'RUNNING', 'PENDING_CLEANUP')
                 )""")) {
            stmt.setLong(1, jobId);
            stmt.setLong(2, jobId);
            stmt.setTimestamp(3, Timestamp.from(Instant.now()));
            stmt.setLong(4, jobId);
            stmt.setLong(5, jobId);
            stmt.executeUpdate();
        }
    }
}
