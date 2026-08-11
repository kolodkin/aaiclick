package io.aaiclick.worker.logs;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.aaiclick.worker.ch.ChClient;
import io.aaiclick.worker.config.WorkerConfig;
import io.aaiclick.worker.testsupport.Backends;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LogFlusherTest {

    private ChClient client() {
        Backends.ChBackend ch = Backends.ch();
        String authority = ch.user().isEmpty() ? "" : ch.user() + ":" + ch.password() + "@";
        WorkerConfig cfg = WorkerConfig.fromEnv(Map.of(
            "AAICLICK_SQL_URL", "postgresql+asyncpg://x:x@unused:5432/x",
            "AAICLICK_CH_URL", "clickhouse://" + authority + ch.httpUrl().replaceFirst("^http://", "") + "/default"
        ));
        return new ChClient(cfg);
    }

    @BeforeEach
    void createTaskLogs() {
        ChClient ch = client();
        ch.query("""
            CREATE TABLE IF NOT EXISTS task_logs (
                task_id UInt64, job_id UInt64, run_id UInt64, seq UInt64,
                stream String, level String, line String, created_at DateTime64(3)
            ) ENGINE = MergeTree() ORDER BY (task_id, run_id, seq)""");
        ch.query("TRUNCATE TABLE task_logs");
    }

    @Test
    void flushWritesLinesWithIncreasingSeq() {
        ChClient ch = client();
        LogFlusher flusher = new LogFlusher(ch, 100L, 10L, 555L);
        flusher.add(LogLine.stdout("first"));
        flusher.add(LogLine.stderr("second"));
        flusher.flush();
        flusher.add(LogLine.stdout("third"));
        flusher.close();

        String rows = ch.query(
            "SELECT seq, stream, level, line FROM task_logs"
            + " WHERE task_id = 100 AND run_id = 555 ORDER BY seq");
        assertEquals("0\tstdout\tINFO\tfirst\n1\tstderr\tWARNING\tsecond\n2\tstdout\tINFO\tthird", rows);

        // created_at must parse as a real timestamp, not fall back to epoch 0
        assertEquals("3", ch.query(
            "SELECT COUNT(*) FROM task_logs WHERE task_id = 100 AND run_id = 555"
            + " AND created_at > now() - INTERVAL 1 DAY"));
    }

    @Test
    void emptyFlushIsANoop() {
        ChClient ch = client();
        new LogFlusher(ch, 101L, 10L, 556L).flush();
        assertEquals("0", ch.query("SELECT COUNT(*) FROM task_logs WHERE task_id = 101"));
    }
}
