package io.aaiclick.worker.db;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NamedParamSqlTest {

    @Test
    void convertsNamedParamsToPositionalInOrder() {
        NamedParamSql sql = NamedParamSql.parse("SELECT * FROM t WHERE a = :first AND b = :second");
        assertEquals("SELECT * FROM t WHERE a = ? AND b = ?", sql.jdbcSql());
        assertEquals(List.of("first", "second"), sql.paramOrder());
    }

    @Test
    void repeatedParamGetsOnePositionPerOccurrence() {
        NamedParamSql sql = NamedParamSql.parse("UPDATE t SET a = :now WHERE b <= :now AND c = :id");
        assertEquals("UPDATE t SET a = ? WHERE b <= ? AND c = ?", sql.jdbcSql());
        assertEquals(List.of("now", "now", "id"), sql.paramOrder());
    }

    @Test
    void postgresCastsAreNotParams() {
        NamedParamSql sql = NamedParamSql.parse(
            "SELECT x::text FROM t WHERE y = :val AND z::json IS NOT NULL");
        assertEquals("SELECT x::text FROM t WHERE y = ? AND z::json IS NOT NULL", sql.jdbcSql());
        assertEquals(List.of("val"), sql.paramOrder());
    }

    @Test
    void colonsInsideStringLiteralsAreNotParams() {
        NamedParamSql sql = NamedParamSql.parse("SELECT ':fake' FROM t WHERE a = :real");
        assertEquals("SELECT ':fake' FROM t WHERE a = ?", sql.jdbcSql());
        assertEquals(List.of("real"), sql.paramOrder());
    }

    @Test
    void lineCommentsAreIgnoredIncludingApostrophes() {
        NamedParamSql sql = NamedParamSql.parse(
            "-- header mentions :not_a_param and Python's apostrophe\n"
            + "SELECT * FROM t WHERE a = :real -- trailing :also_not\n"
            + "AND b = :second");
        assertEquals(List.of("real", "second"), sql.paramOrder());
    }

    @Test
    void blockCommentsAreIgnored() {
        NamedParamSql sql = NamedParamSql.parse(
            "/* block with :fake and a quote ' inside */ SELECT :real FROM t");
        assertEquals(List.of("real"), sql.paramOrder());
    }

    @Test
    void parsesTheSharedClaimQuery() {
        NamedParamSql sql = NamedParamSql.fromResource("/aaiclick-sql/claim_next_task.sql");
        assertEquals(
            List.of("execution_worker_id", "now", "now", "entry_types", "allow_image_tasks", "now"),
            sql.paramOrder());
    }

    @Test
    void missingResourceThrows() {
        assertThrows(IllegalStateException.class, () -> NamedParamSql.fromResource("/nope.sql"));
    }
}
