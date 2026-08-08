package io.aaiclick.worker.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class TaskRepoClaimTest extends PgTestBase {

    @BeforeEach
    void clean() throws SQLException {
        try (Connection conn = db().connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM dependencies; DELETE FROM tasks; DELETE FROM groups;"
                + " DELETE FROM jobs; DELETE FROM execution_workers;");
        }
    }

    @Test
    void claimsOldestEligibleShellTask() throws SQLException {
        WorkerRepo workers = new WorkerRepo(db());
        workers.register(1L, "h", 1);
        Fixtures.insertJob(db(), 10L, "PENDING");
        Fixtures.insertShellTask(db(), 100L, 10L, "PENDING", "[\"echo\", \"hi\"]");

        ClaimedTask claimed = new TaskRepo(db()).claimNext(1L);
        assertEquals(100L, claimed.id());
        assertEquals(java.util.List.of("echo", "hi"), claimed.command());
        assertEquals(0L, claimed.runEpoch());

        try (Connection conn = db().connect(); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT t.status, t.execution_worker_id, j.status FROM tasks t"
                 + " JOIN jobs j ON j.id = t.job_id WHERE t.id = 100")) {
            rs.next();
            assertEquals("RUNNING", rs.getString(1));
            assertEquals(1L, rs.getLong(2));
            assertEquals("RUNNING", rs.getString(3));  // claim transitions job PENDING -> RUNNING
        }
    }

    @Test
    void skipsModuleTasks() throws SQLException {
        new WorkerRepo(db()).register(1L, "h", 1);
        Fixtures.insertJob(db(), 10L, "PENDING");
        Fixtures.insertModuleTask(db(), 100L, 10L);
        assertNull(new TaskRepo(db()).claimNext(1L));
    }

    @Test
    void claimsTaskWithJsonNullImageSource() throws SQLException {
        // SQLAlchemy stores absent image_source as JSON null, not SQL NULL
        new WorkerRepo(db()).register(1L, "h", 1);
        Fixtures.insertJob(db(), 10L, "PENDING");
        Fixtures.insertShellTask(db(), 100L, 10L, "PENDING", "[\"echo\", \"hi\"]");
        try (Connection conn = db().connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("UPDATE tasks SET image_source = 'null'::json WHERE id = 100");
        }
        assertEquals(100L, new TaskRepo(db()).claimNext(1L).id());
    }

    @Test
    void skipsContainerShellTasks() throws SQLException {
        new WorkerRepo(db()).register(1L, "h", 1);
        Fixtures.insertJob(db(), 10L, "PENDING");
        Fixtures.insertShellTask(db(), 100L, 10L, "PENDING", "[\"echo\", \"hi\"]");
        try (Connection conn = db().connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("UPDATE tasks SET image_source = '{\"kind\": \"prebuilt\"}'::json WHERE id = 100");
        }
        assertNull(new TaskRepo(db()).claimNext(1L));
    }

    @Test
    void respectsTaskDependencies() throws SQLException {
        new WorkerRepo(db()).register(1L, "h", 1);
        Fixtures.insertJob(db(), 10L, "PENDING");
        Fixtures.insertShellTask(db(), 100L, 10L, "PENDING", "[\"echo\", \"up\"]");
        Fixtures.insertShellTask(db(), 101L, 10L, "PENDING", "[\"echo\", \"down\"]");
        Fixtures.insertDependency(db(), 100L, "task", 101L, "task");

        // upstream not COMPLETED -> only 100 claimable
        assertEquals(100L, new TaskRepo(db()).claimNext(1L).id());
        assertNull(new TaskRepo(db()).claimNext(1L));

        try (Connection conn = db().connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("UPDATE tasks SET status = 'COMPLETED' WHERE id = 100");
        }
        assertEquals(101L, new TaskRepo(db()).claimNext(1L).id());
    }

    @Test
    void skipsCancelledAndFailedJobs() throws SQLException {
        new WorkerRepo(db()).register(1L, "h", 1);
        Fixtures.insertJob(db(), 10L, "CANCELLED");
        Fixtures.insertShellTask(db(), 100L, 10L, "PENDING", "[\"echo\", \"hi\"]");
        assertNull(new TaskRepo(db()).claimNext(1L));
    }

    @Test
    void respectsRetryAfter() throws SQLException {
        new WorkerRepo(db()).register(1L, "h", 1);
        Fixtures.insertJob(db(), 10L, "PENDING");
        Fixtures.insertShellTask(db(), 100L, 10L, "PENDING", "[\"echo\", \"hi\"]");
        try (Connection conn = db().connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("UPDATE tasks SET retry_after = now() + interval '1 hour' WHERE id = 100");
        }
        assertNull(new TaskRepo(db()).claimNext(1L));
    }
}
