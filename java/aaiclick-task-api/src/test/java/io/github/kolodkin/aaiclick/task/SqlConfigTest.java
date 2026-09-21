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
    void leavesAnAbsentPortToTheDriver() {
        SqlConfig cfg = SqlConfig.fromUrl("postgresql://u@db/orch");
        assertEquals("jdbc:postgresql://db/orch", cfg.jdbcUrl());
        assertEquals("", cfg.password());
    }

    @Test
    void decodesPercentEncodedCredentials() {
        SqlConfig cfg = SqlConfig.fromUrl("postgresql+asyncpg://user%40corp:p%40ss@db:5432/orch");
        assertEquals("user@corp", cfg.user());
        assertEquals("p@ss", cfg.password());
    }

    @Test
    void keepsPlusSignInCredentials() {
        SqlConfig cfg = SqlConfig.fromUrl("postgresql+asyncpg://u+ser:p+ss@db:5432/orch");
        assertEquals("u+ser", cfg.user());
        assertEquals("p+ss", cfg.password());
        assertEquals("p+ss", SqlConfig.fromUrl("postgresql://u:p%2Bss@db/orch").password());
    }

    @Test
    void splitsCredentialsOnFirstRawColon() {
        SqlConfig cfg = SqlConfig.fromUrl("postgresql://u:p%3Aa:b@db/orch");
        assertEquals("u", cfg.user());
        assertEquals("p:a:b", cfg.password());
    }

    @Test
    void mapsSslModeAndForwardsTheRestOfTheQuery() {
        SqlConfig cfg = SqlConfig.fromUrl("postgresql+asyncpg://u:p@db:5432/orch?ssl=require&application_name=x");
        assertEquals("jdbc:postgresql://db:5432/orch?sslmode=require&application_name=x", cfg.jdbcUrl());
        assertEquals("jdbc:postgresql://db/orch?a=1&sslmode=verify-full",
            SqlConfig.fromUrl("postgresql://u:p@db/orch?a=1&ssl=verify-full").jdbcUrl());
    }

    @Test
    void passwordMayHoldSlashAndQuestionMark() {
        SqlConfig cfg = SqlConfig.fromUrl("postgresql://u:p/w?x@db/orch");
        assertEquals("p/w?x", cfg.password());
        assertEquals("jdbc:postgresql://db/orch", cfg.jdbcUrl());
    }

    @Test
    void atSignInQueryIsNotCredentials() {
        SqlConfig cfg = SqlConfig.fromUrl("postgresql://db/orch?application_name=a@b");
        assertEquals("", cfg.user());
        assertEquals("jdbc:postgresql://db/orch?application_name=a@b", cfg.jdbcUrl());
    }

    @Test
    void keepsInvalidPercentEscapesVerbatim() {
        assertEquals("p%zz", SqlConfig.fromUrl("postgresql://u:p%zz@db/orch").password());
        assertEquals("100%", SqlConfig.fromUrl("postgresql://u:100%@db/orch").password());
    }

    @Test
    void acceptsUnderscoredHostname() {
        SqlConfig cfg = SqlConfig.fromUrl("postgresql+asyncpg://u:p@postgres_db:5432/orch");
        assertEquals("jdbc:postgresql://postgres_db:5432/orch", cfg.jdbcUrl());
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
        assertThrows(IllegalArgumentException.class, () -> SqlConfig.fromUrl("postgresql:nonsense"));
        assertThrows(IllegalArgumentException.class, () -> SqlConfig.fromEnv(Map.of()));
    }
}
