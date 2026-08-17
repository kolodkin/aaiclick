package io.aaiclick.worker.db;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeAll;

import io.aaiclick.worker.config.WorkerConfig;
import io.aaiclick.worker.testsupport.Backends;

/** Base for repo tests: a Db against the test Postgres, fresh schema per class. */
public abstract class PgTestBase {

    protected static Db db() {
        Backends.PgBackend pg = Backends.pg();
        WorkerConfig cfg = new WorkerConfig(
            pg.jdbcUrl(), pg.user(), pg.password(),
            "http://unused:8123", "", "", "default", null);
        return new Db(cfg);
    }

    @BeforeAll
    static void loadSchema() throws SQLException, IOException {
        String ddl = new String(
            PgTestBase.class.getResourceAsStream("/schema.sql").readAllBytes(), StandardCharsets.UTF_8);
        try (Connection conn = db().connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;");
            stmt.execute(ddl);
        }
    }
}
