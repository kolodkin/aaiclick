package io.github.kolodkin.aaiclick.task.testsupport;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Per-test SQLite databases for the shim suite — no infrastructure needed.
 *
 * <p>By default each database is created from {@code schema-sqlite.sql}, the
 * local mirror of the SQLModel subset the shim touches. When
 * {@code AAICLICK_TEST_SQLITE_DB} points at a template file (CI generates one
 * from the Python metadata), it is copied instead — the cross-language drift
 * guard.
 */
public final class TestDb {

    public record ResultRow(boolean success, String resultRef, String error) {}

    private TestDb() {
    }

    public static Path create() throws IOException, SQLException {
        Path file = Files.createTempFile("aaiclick-task-api-", ".db");
        String template = System.getenv("AAICLICK_TEST_SQLITE_DB");
        if (template != null && !template.isEmpty()) {
            Files.copy(Path.of(template), file, StandardCopyOption.REPLACE_EXISTING);
            return file;
        }
        try (Connection conn = connect(file); Statement stmt = conn.createStatement()) {
            for (String ddl : readSchema().split(";")) {
                if (!ddl.isBlank()) {
                    stmt.execute(ddl);
                }
            }
        }
        return file;
    }

    /** The aaiclick-format URL for the file, as AAICLICK_SQL_URL expects. */
    public static String sqlUrl(Path file) {
        return "sqlite+aiosqlite:///" + file.toAbsolutePath();
    }

    public static Connection connect(Path file) throws SQLException {
        return DriverManager.getConnection("jdbc:sqlite:" + file.toAbsolutePath());
    }

    public static void insertTask(
            Connection conn, long id, String entrypoint, String kwargsJson, String status, String resultJson)
            throws SQLException {
        String sql = "INSERT INTO tasks (id, job_id, entrypoint, name, kwargs, entry_type, status, "
            + "created_at, max_retries, attempt, result, run_epoch, run_ids, run_statuses, is_image_build) "
            + "VALUES (?, 1, ?, ?, ?, 'jvm', ?, CURRENT_TIMESTAMP, 0, 0, ?, 0, '[]', '[]', 0)";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, id);
            stmt.setString(2, entrypoint);
            stmt.setString(3, entrypoint.isEmpty() ? "task" : entrypoint);
            stmt.setString(4, kwargsJson);
            stmt.setString(5, status);
            stmt.setString(6, resultJson);
            stmt.executeUpdate();
        }
    }

    public static ResultRow readResultRow(Connection conn, long taskId, long runEpoch) throws SQLException {
        String sql = "SELECT success, result_ref, error FROM remote_task_results "
            + "WHERE task_id = ? AND run_epoch = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, taskId);
            stmt.setLong(2, runEpoch);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return new ResultRow(rs.getBoolean("success"), rs.getString("result_ref"), rs.getString("error"));
            }
        }
    }

    private static String readSchema() throws IOException {
        try (InputStream in = TestDb.class.getResourceAsStream("/schema-sqlite.sql")) {
            if (in == null) {
                throw new IllegalStateException("schema-sqlite.sql not found on the test classpath");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
