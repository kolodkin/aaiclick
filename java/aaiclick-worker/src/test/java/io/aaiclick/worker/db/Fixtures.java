package io.aaiclick.worker.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

/** Plain JDBC insert helpers for claim/lifecycle tests. */
public final class Fixtures {

    private Fixtures() {}

    public static void insertJob(Db db, long id, String status) throws SQLException {
        try (Connection conn = db.connect();
             PreparedStatement stmt = conn.prepareStatement(
                 "INSERT INTO jobs (id, name, status, created_at) VALUES (?, ?, ?, ?)")) {
            stmt.setLong(1, id);
            stmt.setString(2, "job-" + id);
            stmt.setString(3, status);
            stmt.setTimestamp(4, Timestamp.from(Instant.now()));
            stmt.executeUpdate();
        }
    }

    public static void insertShellTask(Db db, long id, long jobId, String status, String commandJson)
            throws SQLException {
        insertTask(db, id, jobId, status, "shell", commandJson);
    }

    public static void insertModuleTask(Db db, long id, long jobId) throws SQLException {
        insertTask(db, id, jobId, "PENDING", "module", null);
    }

    private static void insertTask(Db db, long id, long jobId, String status, String entryType, String commandJson)
            throws SQLException {
        try (Connection conn = db.connect();
             PreparedStatement stmt = conn.prepareStatement(
                 "INSERT INTO tasks (id, job_id, entrypoint, name, kwargs, entry_type, command, status, created_at)"
                 + " VALUES (?, ?, '', ?, '{}', ?, ?::json, ?, ?)")) {
            stmt.setLong(1, id);
            stmt.setLong(2, jobId);
            stmt.setString(3, "task-" + id);
            stmt.setString(4, entryType);
            stmt.setString(5, commandJson);
            stmt.setString(6, status);
            stmt.setTimestamp(7, Timestamp.from(Instant.now()));
            stmt.executeUpdate();
        }
    }

    public static void insertDependency(Db db, long prevId, String prevType, long nextId, String nextType)
            throws SQLException {
        try (Connection conn = db.connect();
             PreparedStatement stmt = conn.prepareStatement(
                 "INSERT INTO dependencies (previous_id, previous_type, next_id, next_type, created_at)"
                 + " VALUES (?, ?, ?, ?, ?)")) {
            stmt.setLong(1, prevId);
            stmt.setString(2, prevType);
            stmt.setLong(3, nextId);
            stmt.setString(4, nextType);
            stmt.setTimestamp(5, Timestamp.from(Instant.now()));
            stmt.executeUpdate();
        }
    }
}
