package io.github.kolodkin.aaiclick.task;

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
    void parsesTheShimSqlResources() {
        assertEquals(List.of("task_id"),
            NamedParamSql.fromResource("sql/select_task.sql").paramOrder());
        assertEquals(List.of("task_id"),
            NamedParamSql.fromResource("sql/select_task_result.sql").paramOrder());
        // CAST(:result_ref AS json) must still bind the named param.
        assertEquals(List.of("task_id", "run_epoch", "success", "result_ref", "error"),
            NamedParamSql.fromResource("sql/upsert_remote_task_result_postgres.sql").paramOrder());
        assertEquals(List.of("task_id", "run_epoch", "success", "result_ref", "error"),
            NamedParamSql.fromResource("sql/upsert_remote_task_result_sqlite.sql").paramOrder());
    }

    @Test
    void missingResourceThrows() {
        assertThrows(IllegalStateException.class, () -> NamedParamSql.fromResource("/nope.sql"));
    }
}
