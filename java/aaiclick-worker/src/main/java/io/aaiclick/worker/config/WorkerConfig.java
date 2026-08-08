package io.aaiclick.worker.config;

import java.net.URI;
import java.util.Map;

/** Environment-driven configuration, mirroring aaiclick/backend.py URL formats.
 *
 * AAICLICK_SQL_URL: postgresql+asyncpg://user:pass@host:port/db
 * AAICLICK_CH_URL:  clickhouse://user:pass@host:port/db (port defaults to 8123,
 * the HTTP interface; db defaults to "default").
 */
public record WorkerConfig(
    String jdbcUrl,
    String dbUser,
    String dbPassword,
    String chHttpUrl,
    String chUser,
    String chPassword,
    String chDatabase,
    Double taskTimeoutSeconds
) {

    public static WorkerConfig fromEnv(Map<String, String> env) {
        String sqlUrl = require(env, "AAICLICK_SQL_URL");
        String chUrl = require(env, "AAICLICK_CH_URL");
        if (sqlUrl.startsWith("sqlite")) {
            throw new IllegalArgumentException(
                "AAICLICK_SQL_URL is sqlite — the Java worker is distributed-only; point it at PostgreSQL");
        }
        if (chUrl.startsWith("chdb://")) {
            throw new IllegalArgumentException(
                "AAICLICK_CH_URL is chdb — the Java worker is distributed-only; point it at a ClickHouse server");
        }

        URI sql = URI.create(sqlUrl.replaceFirst("^postgresql\\+[a-z0-9]+", "postgresql"));
        String[] sqlUserInfo = splitUserInfo(sql.getUserInfo());
        String jdbc = "jdbc:postgresql://" + sql.getHost() + ":" + (sql.getPort() == -1 ? 5432 : sql.getPort())
            + sql.getPath();

        URI ch = URI.create(chUrl);
        String[] chUserInfo = splitUserInfo(ch.getUserInfo());
        String chHttp = "http://" + ch.getHost() + ":" + (ch.getPort() == -1 ? 8123 : ch.getPort());
        String chDb = (ch.getPath() == null || ch.getPath().isEmpty() || ch.getPath().equals("/"))
            ? "default" : ch.getPath().substring(1);

        String rawTimeout = env.get("AAICLICK_TASK_TIMEOUT");
        Double timeout = (rawTimeout == null || rawTimeout.isEmpty()) ? null : Double.parseDouble(rawTimeout);

        return new WorkerConfig(jdbc, sqlUserInfo[0], sqlUserInfo[1], chHttp, chUserInfo[0], chUserInfo[1], chDb,
            timeout);
    }

    private static String require(Map<String, String> env, String key) {
        String value = env.get(key);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(key + " must be set for the Java worker");
        }
        return value;
    }

    private static String[] splitUserInfo(String userInfo) {
        if (userInfo == null) {
            return new String[] {"", ""};
        }
        int colon = userInfo.indexOf(':');
        return colon == -1
            ? new String[] {userInfo, ""}
            : new String[] {userInfo.substring(0, colon), userInfo.substring(colon + 1)};
    }
}
