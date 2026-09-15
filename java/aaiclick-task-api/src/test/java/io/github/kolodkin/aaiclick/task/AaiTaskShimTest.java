package io.github.kolodkin.aaiclick.task;

import java.nio.file.Path;
import java.sql.Connection;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.github.kolodkin.aaiclick.task.testsupport.TestDb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** End-to-end shim runs against a per-test SQLite database: task row in,
 * remote_task_results row out — the same contract the host worker reads. */
class AaiTaskShimTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String SAMPLE = "io.github.kolodkin.aaiclick.task.testsupport.SampleTasks";

    private Path db;
    private Connection conn;

    @BeforeEach
    void setUp() throws Exception {
        db = TestDb.create();
        conn = TestDb.connect(db);
    }

    @AfterEach
    void tearDown() throws Exception {
        conn.close();
    }

    private int runShim(long taskId, long runEpoch) {
        String[] args = {"--task-id", String.valueOf(taskId), "--run-epoch", String.valueOf(runEpoch)};
        return AaiTaskShim.run(args, Map.of("AAICLICK_SQL_URL", TestDb.sqlUrl(db)));
    }

    @Test
    void successWritesNativeValueResultRow() throws Exception {
        TestDb.insertTask(conn, 42, SAMPLE + "#aggregate",
            "{\"date\": \"2026-08-20\", \"window\": 7}", "RUNNING", null);

        assertEquals(0, runShim(42, 3));

        TestDb.ResultRow row = TestDb.readResultRow(conn, 42, 3);
        assertTrue(row.success());
        assertNull(row.error());
        assertEquals(MAPPER.readTree("{\"native_value\": {\"date\": \"2026-08-20\", \"window\": 14}}"),
            MAPPER.readTree(row.resultRef()));
    }

    @Test
    void voidResultWritesNullResultRef() throws Exception {
        TestDb.insertTask(conn, 43, SAMPLE + "#sideEffectOnly", "{\"ignored\": \"x\"}", "RUNNING", null);

        assertEquals(0, runShim(43, 0));

        TestDb.ResultRow row = TestDb.readResultRow(conn, 43, 0);
        assertTrue(row.success());
        assertNull(row.resultRef());
    }

    @Test
    void taskExceptionWritesFailureRowWithCause() throws Exception {
        TestDb.insertTask(conn, 44, SAMPLE + "#explode", "{\"message\": \"boom\"}", "RUNNING", null);

        assertEquals(1, runShim(44, 0));

        TestDb.ResultRow row = TestDb.readResultRow(conn, 44, 0);
        assertFalse(row.success());
        assertNull(row.resultRef());
        assertEquals("IllegalStateException: boom", row.error());
    }

    @Test
    void upstreamRefResolvesToCompletedResult() throws Exception {
        TestDb.insertTask(conn, 10, "", null, "COMPLETED", "{\"native_value\": [1, 2.5, 3]}");
        TestDb.insertTask(conn, 45, SAMPLE + "#sum",
            "{\"values\": {\"ref_type\": \"upstream\", \"task_id\": 10}}", "RUNNING", null);

        assertEquals(0, runShim(45, 0));

        TestDb.ResultRow row = TestDb.readResultRow(conn, 45, 0);
        assertTrue(row.success());
        assertEquals(MAPPER.readTree("{\"native_value\": 6.5}"), MAPPER.readTree(row.resultRef()));
    }

    @Test
    void incompleteUpstreamFailsTheTask() throws Exception {
        TestDb.insertTask(conn, 11, "", null, "RUNNING", null);
        TestDb.insertTask(conn, 46, SAMPLE + "#sum",
            "{\"values\": {\"ref_type\": \"upstream\", \"task_id\": 11}}", "RUNNING", null);

        assertEquals(1, runShim(46, 0));

        TestDb.ResultRow row = TestDb.readResultRow(conn, 46, 0);
        assertFalse(row.success());
        assertTrue(row.error().contains("not completed"));
    }

    @Test
    void objectRefKwargFailsTheTask() throws Exception {
        TestDb.insertTask(conn, 47, SAMPLE + "#sum",
            "{\"values\": {\"object_type\": \"object\", \"table\": \"t_1\", \"persistent\": false}}",
            "RUNNING", null);

        assertEquals(1, runShim(47, 0));

        TestDb.ResultRow row = TestDb.readResultRow(conn, 47, 0);
        assertFalse(row.success());
        assertTrue(row.error().contains("plain values only"));
    }

    @Test
    void reRunUnderSameEpochOverwritesTheRow() throws Exception {
        TestDb.insertTask(conn, 48, SAMPLE + "#explode", "{\"message\": \"first\"}", "RUNNING", null);
        assertEquals(1, runShim(48, 5));

        try (var stmt = conn.prepareStatement("UPDATE tasks SET entrypoint = ?, kwargs = ? WHERE id = 48")) {
            stmt.setString(1, SAMPLE + "#aggregate");
            stmt.setString(2, "{\"date\": \"d\", \"window\": 1}");
            stmt.executeUpdate();
        }
        assertEquals(0, runShim(48, 5));

        TestDb.ResultRow row = TestDb.readResultRow(conn, 48, 5);
        assertTrue(row.success());
        assertNull(row.error());
    }

    @Test
    void unknownEntrypointWritesFailureRow() throws Exception {
        TestDb.insertTask(conn, 49, "com.example.Missing", "{}", "RUNNING", null);

        assertEquals(1, runShim(49, 0));

        TestDb.ResultRow row = TestDb.readResultRow(conn, 49, 0);
        assertFalse(row.success());
        assertTrue(row.error().contains("com.example.Missing"));
    }

    @Test
    void unreachableDatabaseWritesNoRowAndFails() {
        String[] args = {"--task-id", "1", "--run-epoch", "0"};
        int exit = AaiTaskShim.run(args, Map.of("AAICLICK_SQL_URL", "mysql://nope/db"));
        assertEquals(1, exit);
    }
}
