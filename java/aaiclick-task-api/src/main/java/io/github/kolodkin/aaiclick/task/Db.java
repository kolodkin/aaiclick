package io.github.kolodkin.aaiclick.task;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/** JDBC connection factory for the orchestration SQL database. */
public class Db {

    private final String jdbcUrl;
    private final Properties props;

    public Db(SqlConfig cfg) {
        this.jdbcUrl = cfg.jdbcUrl();
        this.props = new Properties();
        if (!cfg.user().isEmpty()) {
            props.setProperty("user", cfg.user());
            props.setProperty("password", cfg.password());
        }
    }

    public Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, props);
    }
}
