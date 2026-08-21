package io.github.kolodkin.aaiclick.task;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SQL access for the shim: task-row load, upstream-result load, and the
 * {@code remote_task_results} upsert the host worker reads back.
 *
 * <p>The shim never writes terminal {@code Task} status — the host worker
 * owns that (the reaper invariant). A stale attempt writes under a stale
 * {@code (task_id, run_epoch)} key and is ignored.
 */
public class TaskStore {

    public static final String TASK_COMPLETED = "COMPLETED";

    private static final NamedParamSql SELECT_TASK = NamedParamSql.fromResource("sql/select_task.sql");
    private static final NamedParamSql SELECT_RESULT = NamedParamSql.fromResource("sql/select_task_result.sql");
    private static final NamedParamSql UPSERT_POSTGRES =
        NamedParamSql.fromResource("sql/upsert_remote_task_result_postgres.sql");
    private static final NamedParamSql UPSERT_SQLITE =
        NamedParamSql.fromResource("sql/upsert_remote_task_result_sqlite.sql");

    private final Connection conn;
    private final SqlConfig.Dialect dialect;

    public TaskStore(Connection conn, SqlConfig.Dialect dialect) {
        this.conn = conn;
        this.dialect = dialect;
    }

    public TaskRow loadTask(long taskId) throws SQLException {
        try (PreparedStatement stmt = prepare(SELECT_TASK, Map.of("task_id", taskId));
             ResultSet rs = stmt.executeQuery()) {
            if (!rs.next()) {
                throw new IllegalStateException("Task " + taskId + " not found");
            }
            return new TaskRow(rs.getLong("id"), rs.getLong("job_id"),
                rs.getString("entrypoint"), rs.getString("kwargs"));
        }
    }

    /** The ``result`` JSON of a COMPLETED upstream task (null for a null
     * result), mirroring the Python ``_resolve_upstream_ref``. */
    public String loadCompletedResult(long taskId) throws SQLException {
        try (PreparedStatement stmt = prepare(SELECT_RESULT, Map.of("task_id", taskId));
             ResultSet rs = stmt.executeQuery()) {
            if (!rs.next()) {
                throw new IllegalStateException("Upstream task " + taskId + " not found");
            }
            String status = rs.getString("status");
            if (!TASK_COMPLETED.equals(status)) {
                throw new IllegalStateException(
                    "Upstream task " + taskId + " is not completed (status: " + status + ")");
            }
            return rs.getString("result");
        }
    }

    /** Upsert the per-attempt result row keyed ``(task_id, run_epoch)`` so a
     * re-run under a new epoch never collides. */
    public void writeResult(long taskId, long runEpoch, boolean success, String resultRefJson, String error)
            throws SQLException {
        NamedParamSql sql = dialect == SqlConfig.Dialect.POSTGRES ? UPSERT_POSTGRES : UPSERT_SQLITE;
        HashMap<String, Object> params = new HashMap<>();
        params.put("task_id", taskId);
        params.put("run_epoch", runEpoch);
        params.put("success", success);
        params.put("result_ref", resultRefJson);
        params.put("error", error);
        try (PreparedStatement stmt = prepare(sql, params)) {
            stmt.executeUpdate();
        }
        if (!conn.getAutoCommit()) {
            conn.commit();
        }
    }

    private PreparedStatement prepare(NamedParamSql sql, Map<String, Object> params) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(sql.jdbcSql());
        List<String> order = sql.paramOrder();
        for (int i = 0; i < order.size(); i++) {
            stmt.setObject(i + 1, params.get(order.get(i)));
        }
        return stmt;
    }
}
