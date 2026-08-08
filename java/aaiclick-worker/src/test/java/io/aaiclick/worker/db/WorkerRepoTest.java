package io.aaiclick.worker.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class WorkerRepoTest extends PgTestBase {

    @Test
    void registerHeartbeatStopLifecycle() throws SQLException {
        WorkerRepo repo = new WorkerRepo(db());
        repo.register(101L, "host-a", 4242);
        assertEquals("ACTIVE", repo.heartbeat(101L));

        // an external `execution-worker stop` sets STOPPING; heartbeat must not undo it
        try (Connection conn = db().connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("UPDATE execution_workers SET status = 'STOPPING' WHERE id = 101");
        }
        assertEquals("STOPPING", repo.heartbeat(101L));

        repo.markStopped(101L);
        try (Connection conn = db().connect(); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT status FROM execution_workers WHERE id = 101")) {
            rs.next();
            assertEquals("STOPPED", rs.getString(1));
        }
    }

    @Test
    void heartbeatOnMissingWorkerReturnsNull() throws SQLException {
        assertNull(new WorkerRepo(db()).heartbeat(999L));
    }

    @Test
    void countersIncrement() throws SQLException {
        WorkerRepo repo = new WorkerRepo(db());
        repo.register(102L, "host-b", 1);
        repo.bumpCompleted(102L);
        repo.bumpCompleted(102L);
        repo.bumpFailed(102L);
        try (Connection conn = db().connect(); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT tasks_completed, tasks_failed FROM execution_workers WHERE id = 102")) {
            rs.next();
            assertEquals(2, rs.getInt(1));
            assertEquals(1, rs.getInt(2));
        }
    }
}
