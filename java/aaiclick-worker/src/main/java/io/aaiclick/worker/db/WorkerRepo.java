package io.aaiclick.worker.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

/** execution_workers row lifecycle: register, heartbeat, counters, stop. */
public class WorkerRepo {

    private final Db db;

    public WorkerRepo(Db db) {
        this.db = db;
    }

    public void register(long workerId, String hostname, int pid) throws SQLException {
        try (Connection conn = db.connect();
             PreparedStatement stmt = conn.prepareStatement(
                 "INSERT INTO execution_workers"
                 + " (id, hostname, pid, status, created_at, started_at, last_heartbeat,"
                 + "  tasks_completed, tasks_failed)"
                 + " VALUES (?, ?, ?, 'ACTIVE', ?, ?, ?, 0, 0)")) {
            Timestamp now = Timestamp.from(Instant.now());
            stmt.setLong(1, workerId);
            stmt.setString(2, hostname);
            stmt.setInt(3, pid);
            stmt.setTimestamp(4, now);
            stmt.setTimestamp(5, now);
            stmt.setTimestamp(6, now);
            stmt.executeUpdate();
        }
    }

    public String heartbeat(long workerId) throws SQLException {
        try (Connection conn = db.connect();
             PreparedStatement stmt = conn.prepareStatement(
                 "UPDATE execution_workers"
                 + " SET last_heartbeat = ?,"
                 + "     status = CASE WHEN status = 'STOPPING' THEN status ELSE 'ACTIVE' END"
                 + " WHERE id = ? RETURNING status")) {
            stmt.setTimestamp(1, Timestamp.from(Instant.now()));
            stmt.setLong(2, workerId);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    public void markStopped(long workerId) throws SQLException {
        execute("UPDATE execution_workers SET status = 'STOPPED' WHERE id = ?", workerId);
    }

    public void bumpCompleted(long workerId) throws SQLException {
        execute("UPDATE execution_workers SET tasks_completed = tasks_completed + 1 WHERE id = ?", workerId);
    }

    public void bumpFailed(long workerId) throws SQLException {
        execute("UPDATE execution_workers SET tasks_failed = tasks_failed + 1 WHERE id = ?", workerId);
    }

    private void execute(String sql, long workerId) throws SQLException {
        try (Connection conn = db.connect(); PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, workerId);
            stmt.executeUpdate();
        }
    }
}
