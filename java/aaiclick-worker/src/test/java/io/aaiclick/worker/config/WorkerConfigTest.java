package io.aaiclick.worker.config;

import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WorkerConfigTest {

    private static final Map<String, String> DISTRIBUTED = Map.of(
        "AAICLICK_SQL_URL", "postgresql+asyncpg://app:secret@pg.example:5432/aaiclick",
        "AAICLICK_CH_URL", "clickhouse://chuser:chpass@ch.example:8123/default"
    );

    @Test
    void parsesDistributedUrls() {
        WorkerConfig cfg = WorkerConfig.fromEnv(DISTRIBUTED);
        assertEquals("jdbc:postgresql://pg.example:5432/aaiclick", cfg.jdbcUrl());
        assertEquals("app", cfg.dbUser());
        assertEquals("secret", cfg.dbPassword());
        assertEquals("http://ch.example:8123", cfg.chHttpUrl());
        assertEquals("chuser", cfg.chUser());
        assertEquals("chpass", cfg.chPassword());
        assertEquals("default", cfg.chDatabase());
        assertNull(cfg.taskTimeoutSeconds());
    }

    @Test
    void defaultsChPortAndDatabase() {
        WorkerConfig cfg = WorkerConfig.fromEnv(Map.of(
            "AAICLICK_SQL_URL", "postgresql+asyncpg://app:secret@pg:5432/aaiclick",
            "AAICLICK_CH_URL", "clickhouse://ch.example"
        ));
        assertEquals("http://ch.example:8123", cfg.chHttpUrl());
        assertEquals("default", cfg.chDatabase());
    }

    @Test
    void parsesTaskTimeout() {
        java.util.HashMap<String, String> env = new java.util.HashMap<>(DISTRIBUTED);
        env.put("AAICLICK_TASK_TIMEOUT", "12.5");
        assertEquals(12.5, WorkerConfig.fromEnv(env).taskTimeoutSeconds());
    }

    @Test
    void rejectsLocalBackends() {
        assertThrows(IllegalArgumentException.class, () -> WorkerConfig.fromEnv(Map.of(
            "AAICLICK_SQL_URL", "sqlite+aiosqlite:///home/u/.aaiclick/local.db",
            "AAICLICK_CH_URL", "clickhouse://ch:8123/default"
        )));
        assertThrows(IllegalArgumentException.class, () -> WorkerConfig.fromEnv(Map.of(
            "AAICLICK_SQL_URL", "postgresql+asyncpg://a:b@pg:5432/db",
            "AAICLICK_CH_URL", "chdb:///home/u/.aaiclick/chdb_data"
        )));
    }

    @Test
    void rejectsMissingUrls() {
        assertThrows(IllegalArgumentException.class, () -> WorkerConfig.fromEnv(Map.of()));
    }
}
