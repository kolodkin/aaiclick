package io.aaiclick.worker.testsupport;

import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

/** Test backends: env-provided servers, or Testcontainers when Docker exists.
 *
 * Set AAICLICK_TEST_PG_JDBC / AAICLICK_TEST_CH_HTTP (plus _USER / _PASS
 * variants) to point tests at externally-managed servers — required in
 * sandboxes without a Docker daemon. Unset, each backend lazily starts a
 * singleton container that Ryuk reaps after the JVM exits.
 */
public final class Backends {

    public record PgBackend(String jdbcUrl, String user, String password) {}

    public record ChBackend(String httpUrl, String user, String password) {}

    private static PgBackend pg;
    private static ChBackend ch;

    private Backends() {}

    public static synchronized PgBackend pg() {
        if (pg == null) {
            String jdbc = System.getenv("AAICLICK_TEST_PG_JDBC");
            if (jdbc != null && !jdbc.isEmpty()) {
                pg = new PgBackend(jdbc, env("AAICLICK_TEST_PG_USER"), env("AAICLICK_TEST_PG_PASS"));
            } else {
                PostgreSQLContainer<?> container = new PostgreSQLContainer<>("postgres:16");
                container.start();
                pg = new PgBackend(container.getJdbcUrl(), container.getUsername(), container.getPassword());
            }
        }
        return pg;
    }

    public static synchronized ChBackend ch() {
        if (ch == null) {
            String http = System.getenv("AAICLICK_TEST_CH_HTTP");
            if (http != null && !http.isEmpty()) {
                ch = new ChBackend(http, env("AAICLICK_TEST_CH_USER"), env("AAICLICK_TEST_CH_PASS"));
            } else {
                ClickHouseContainer container = new ClickHouseContainer("clickhouse/clickhouse-server:24.8");
                container.start();
                ch = new ChBackend(
                    "http://" + container.getHost() + ":" + container.getMappedPort(8123),
                    container.getUsername(), container.getPassword());
            }
        }
        return ch;
    }

    private static String env(String key) {
        String value = System.getenv(key);
        return value == null ? "" : value;
    }
}
