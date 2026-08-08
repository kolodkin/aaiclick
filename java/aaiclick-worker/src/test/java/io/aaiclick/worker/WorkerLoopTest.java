package io.aaiclick.worker;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.aaiclick.worker.ch.ChClient;
import io.aaiclick.worker.config.WorkerConfig;
import io.aaiclick.worker.db.Fixtures;
import io.aaiclick.worker.db.PgTestBase;
import io.aaiclick.worker.testsupport.Backends;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerLoopTest extends PgTestBase {

    private WorkerConfig config() {
        Backends.PgBackend pg = Backends.pg();
        Backends.ChBackend ch = Backends.ch();
        return new WorkerConfig(
            pg.jdbcUrl(), pg.user(), pg.password(),
            ch.httpUrl(), ch.user(), ch.password(), "default", null);
    }

    @BeforeEach
    void clean() throws SQLException {
        try (Connection conn = db().connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM dependencies; DELETE FROM tasks; DELETE FROM groups;"
                + " DELETE FROM jobs; DELETE FROM execution_workers;");
        }
        ChClient ch = new ChClient(config());
        ch.query("""
            CREATE TABLE IF NOT EXISTS task_logs (
                task_id UInt64, job_id UInt64, run_id UInt64, seq UInt64,
                stream String, level String, line String, created_at DateTime64(3)
            ) ENGINE = MergeTree() ORDER BY (task_id, run_id, seq)""");
        ch.query("TRUNCATE TABLE task_logs");
    }

    private String scalar(String sql) throws SQLException {
        try (Connection conn = db().connect(); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }

    @Test
    void runsShellTaskToCompletionWithLogs() throws Exception {
        Fixtures.insertJob(db(), 10L, "PENDING");
        Fixtures.insertShellTask(db(), 100L, 10L, "PENDING", "[\"sh\", \"-c\", \"echo from-java\"]");

        Worker worker = new Worker(config());
        worker.runLoop(1);

        assertEquals("COMPLETED", scalar("SELECT status FROM tasks WHERE id = 100"));
        assertEquals("COMPLETED", scalar("SELECT status FROM jobs WHERE id = 10"));
        assertEquals("STOPPED", scalar(
            "SELECT status FROM execution_workers WHERE id = " + worker.workerId()));
        assertEquals("1", scalar(
            "SELECT tasks_completed FROM execution_workers WHERE id = " + worker.workerId()));
        String logged = new ChClient(config()).query(
            "SELECT line FROM task_logs WHERE task_id = 100 AND stream = 'stdout'");
        assertEquals("from-java", logged);
    }

    @Test
    void failingTaskGoesToPendingCleanup() throws Exception {
        Fixtures.insertJob(db(), 10L, "PENDING");
        Fixtures.insertShellTask(db(), 100L, 10L, "PENDING", "[\"sh\", \"-c\", \"exit 7\"]");

        Worker worker = new Worker(config());
        worker.runLoop(1);

        assertEquals("PENDING_CLEANUP", scalar("SELECT status FROM tasks WHERE id = 100"));
        assertTrue(scalar("SELECT error FROM tasks WHERE id = 100").contains("7"));
        assertEquals("RUNNING", scalar("SELECT status FROM jobs WHERE id = 10"));
        assertEquals("1", scalar(
            "SELECT tasks_failed FROM execution_workers WHERE id = " + worker.workerId()));
    }
}
