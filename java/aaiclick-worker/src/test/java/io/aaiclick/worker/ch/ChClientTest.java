package io.aaiclick.worker.ch;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import io.aaiclick.worker.config.WorkerConfig;
import io.aaiclick.worker.testsupport.Backends;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChClientTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ChClient client() {
        Backends.ChBackend ch = Backends.ch();
        String authority = ch.user().isEmpty() ? "" : ch.user() + ":" + ch.password() + "@";
        WorkerConfig cfg = WorkerConfig.fromEnv(Map.of(
            "AAICLICK_SQL_URL", "postgresql+asyncpg://x:x@unused:5432/x",
            "AAICLICK_CH_URL", "clickhouse://" + authority + ch.httpUrl().replaceFirst("^http://", "") + "/default"
        ));
        return new ChClient(cfg);
    }

    @Test
    void queryReturnsScalar() {
        assertEquals("2", client().query("SELECT 1 + 1"));
    }

    @Test
    void nextSnowflakeIdIsPositiveAndIncreasing() {
        ChClient ch = client();
        long first = ch.nextSnowflakeId();
        long second = ch.nextSnowflakeId();
        assertTrue(first > 0);
        assertTrue(second > first);
    }

    @Test
    void insertJsonEachRowRoundTrips() {
        ChClient ch = client();
        ch.query("DROP TABLE IF EXISTS jer_test");
        ch.query("CREATE TABLE jer_test (a UInt64, b String) ENGINE = MergeTree() ORDER BY a");
        ObjectNode row = MAPPER.createObjectNode();
        row.put("a", 7L);
        row.put("b", "hello");
        ch.insertJsonEachRow("jer_test", List.of(row));
        assertEquals("hello", ch.query("SELECT b FROM jer_test WHERE a = 7"));
    }
}
