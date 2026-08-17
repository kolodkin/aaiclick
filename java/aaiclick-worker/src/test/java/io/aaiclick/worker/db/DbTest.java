package io.aaiclick.worker.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DbTest extends PgTestBase {

    @Test
    void connectsAndQueries() throws SQLException {
        try (Connection conn = db().connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM tasks")) {
            rs.next();
            assertEquals(0, rs.getInt(1));
        }
    }
}
