package io.aaiclick.worker.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskRepoLifecycleTest extends PgTestBase {

    @BeforeEach
    void clean() throws SQLException {
        try (Connection conn = db().connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM dependencies; DELETE FROM tasks; DELETE FROM groups;"
                + " DELETE FROM jobs; DELETE FROM execution_workers;");
        }
    }

    private ClaimedTask claimOne() throws SQLException {
        new WorkerRepo(db()).register(1L, "h", 1);
        Fixtures.insertJob(db(), 10L, "PENDING");
        Fixtures.insertShellTask(db(), 100L, 10L, "PENDING", "[\"echo\", \"hi\"]");
        return new TaskRepo(db()).claimNext(1L);
    }

    private String scalar(String sql) throws SQLException {
        try (Connection conn = db().connect(); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }

    @Test
    void startRunAppendsRunArrays() throws SQLException {
        ClaimedTask task = claimOne();
        new TaskRepo(db()).startRun(task.id(), 555L);
        assertEquals("[555]", scalar("SELECT run_ids::text FROM tasks WHERE id = 100"));
        assertEquals("[\"RUNNING\"]", scalar("SELECT run_statuses::text FROM tasks WHERE id = 100"));
    }

    @Test
    void completeSetsTerminalStateAndRollsUpJob() throws SQLException {
        ClaimedTask task = claimOne();
        TaskRepo repo = new TaskRepo(db());
        repo.startRun(task.id(), 555L);
        assertTrue(repo.complete(task.id(), task.runEpoch()));
        repo.tryCompleteJob(task.jobId());
        assertEquals("COMPLETED", scalar("SELECT status FROM tasks WHERE id = 100"));
        assertEquals("[\"COMPLETED\"]", scalar("SELECT run_statuses::text FROM tasks WHERE id = 100"));
        assertEquals("COMPLETED", scalar("SELECT status FROM jobs WHERE id = 10"));
    }

    @Test
    void failSetsPendingCleanupAndJobStaysRunning() throws SQLException {
        ClaimedTask task = claimOne();
        TaskRepo repo = new TaskRepo(db());
        repo.startRun(task.id(), 555L);
        assertTrue(repo.failPendingCleanup(task.id(), task.runEpoch(), "exit code 3"));
        repo.tryCompleteJob(task.jobId());
        assertEquals("PENDING_CLEANUP", scalar("SELECT status FROM tasks WHERE id = 100"));
        assertEquals("exit code 3", scalar("SELECT error FROM tasks WHERE id = 100"));
        assertEquals("[\"FAILED\"]", scalar("SELECT run_statuses::text FROM tasks WHERE id = 100"));
        // PENDING_CLEANUP is non-terminal: the BackgroundWorker owns the rest
        assertEquals("RUNNING", scalar("SELECT status FROM jobs WHERE id = 10"));
    }

    @Test
    void epochFencingRejectsStaleWrites() throws SQLException {
        ClaimedTask task = claimOne();
        TaskRepo repo = new TaskRepo(db());
        try (Connection conn = db().connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("UPDATE tasks SET run_epoch = run_epoch + 1 WHERE id = 100");  // clear_task
        }
        assertFalse(repo.complete(task.id(), task.runEpoch()));
        assertFalse(repo.failPendingCleanup(task.id(), task.runEpoch(), "late failure"));
        assertEquals("RUNNING", scalar("SELECT status FROM tasks WHERE id = 100"));
    }

    @Test
    void isRunAbortedDetectsCancellationAndEpochBump() throws SQLException {
        ClaimedTask task = claimOne();
        TaskRepo repo = new TaskRepo(db());
        assertFalse(repo.isRunAborted(task.id(), task.runEpoch()));
        try (Connection conn = db().connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("UPDATE tasks SET status = 'CANCELLED' WHERE id = 100");
        }
        assertTrue(repo.isRunAborted(task.id(), task.runEpoch()));
        try (Connection conn = db().connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("UPDATE tasks SET status = 'RUNNING', run_epoch = run_epoch + 1 WHERE id = 100");
        }
        assertTrue(repo.isRunAborted(task.id(), task.runEpoch()));
    }

    @Test
    void tryCompleteJobFailsJobWhenAnyTaskFailed() throws SQLException {
        new WorkerRepo(db()).register(1L, "h", 1);
        Fixtures.insertJob(db(), 10L, "RUNNING");
        Fixtures.insertShellTask(db(), 100L, 10L, "COMPLETED", "[\"echo\"]");
        Fixtures.insertShellTask(db(), 101L, 10L, "FAILED", "[\"echo\"]");
        new TaskRepo(db()).tryCompleteJob(10L);
        assertEquals("FAILED", scalar("SELECT status FROM jobs WHERE id = 10"));
        assertEquals("One or more tasks failed", scalar("SELECT error FROM jobs WHERE id = 10"));
    }
}
