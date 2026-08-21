package io.github.kolodkin.aaiclick.task;

import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SqlConfigTest {

    @Test
    void translatesAsyncpgUrlToJdbc() {
        SqlConfig cfg = SqlConfig.fromUrl("postgresql+asyncpg://aaiclick:secret@db.example:5433/orch");
        assertEquals(SqlConfig.Dialect.POSTGRES, cfg.dialect());
        assertEquals("jdbc:postgresql://db.example:5433/orch", cfg.jdbcUrl());
        assertEquals("aaiclick", cfg.user());
        assertEquals("secret", cfg.password());
    }

    @Test
    void postgresPortDefaultsTo5432() {
        SqlConfig cfg = SqlConfig.fromUrl("postgresql://u@db/orch");
        assertEquals("jdbc:postgresql://db:5432/orch", cfg.jdbcUrl());
        assertEquals("", cfg.password());
    }

    @Test
    void decodesPercentEncodedCredentials() {
        SqlConfig cfg = SqlConfig.fromUrl("postgresql+asyncpg://user%40corp:p%40ss@db:5432/orch");
        assertEquals("user@corp", cfg.user());
        assertEquals("p@ss", cfg.password());
    }

    @Test
    void translatesSqliteRelativeAndAbsolutePaths() {
        assertEquals("jdbc:sqlite:local.db",
            SqlConfig.fromUrl("sqlite+aiosqlite:///local.db").jdbcUrl());
        SqlConfig absolute = SqlConfig.fromUrl("sqlite+aiosqlite:////tmp/w/test.db");
        assertEquals(SqlConfig.Dialect.SQLITE, absolute.dialect());
        assertEquals("jdbc:sqlite:/tmp/w/test.db", absolute.jdbcUrl());
    }

    @Test
    void rejectsUnsupportedSchemeAndMissingEnv() {
        assertThrows(IllegalArgumentException.class, () -> SqlConfig.fromUrl("mysql://u@h/db"));
        assertThrows(IllegalArgumentException.class, () -> SqlConfig.fromEnv(Map.of()));
    }
}
