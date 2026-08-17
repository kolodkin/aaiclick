package io.aaiclick.worker.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import io.aaiclick.worker.config.WorkerConfig;

/** JDBC connection factory for the orchestration PostgreSQL database. */
public class Db {

    private final String jdbcUrl;
    private final Properties props;

    public Db(WorkerConfig cfg) {
        this.jdbcUrl = cfg.jdbcUrl();
        this.props = new Properties();
        props.setProperty("user", cfg.dbUser());
        props.setProperty("password", cfg.dbPassword());
    }

    public Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, props);
    }
}
