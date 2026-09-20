package io.github.kolodkin.aaiclick.task;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * JDBC settings derived from the {@code AAICLICK_SQL_URL} the runner injects.
 *
 * <p>Mirrors aaiclick's SQLAlchemy URL formats:
 * {@code postgresql+asyncpg://user:pass@host:port/db} (production containers)
 * and {@code sqlite+aiosqlite:///path} (local files, used by the SDK tests).
 */
public record SqlConfig(Dialect dialect, String jdbcUrl, String user, String password) {

    public enum Dialect { POSTGRES, SQLITE }

    public static SqlConfig fromEnv(Map<String, String> env) {
        String url = env.get("AAICLICK_SQL_URL");
        if (url == null || url.isEmpty()) {
            throw new IllegalArgumentException("AAICLICK_SQL_URL must be set for the aaiclick task shim");
        }
        return fromUrl(url);
    }

    public static SqlConfig fromUrl(String url) {
        if (url.startsWith("postgresql")) {
            URI parsed = URI.create(url.replaceFirst("^postgresql(\\+[a-z0-9]+)?", "postgresql"));
            String[] userInfo = splitUserInfo(parsed.getUserInfo());
            String jdbc = "jdbc:postgresql://" + parsed.getHost()
                + ":" + (parsed.getPort() == -1 ? 5432 : parsed.getPort())
                + parsed.getPath();
            return new SqlConfig(Dialect.POSTGRES, jdbc, userInfo[0], userInfo[1]);
        }
        if (url.startsWith("sqlite")) {
            int schemeEnd = url.indexOf("://");
            if (schemeEnd == -1) {
                throw new IllegalArgumentException("Unparseable sqlite URL: " + url);
            }
            // sqlite:///rel.db → "rel.db"; sqlite:////abs/p.db → "/abs/p.db"
            String path = url.substring(schemeEnd + 3);
            if (path.startsWith("/")) {
                path = path.substring(1);
            }
            return new SqlConfig(Dialect.SQLITE, "jdbc:sqlite:" + path, "", "");
        }
        throw new IllegalArgumentException(
            "Unsupported AAICLICK_SQL_URL scheme (expected postgresql or sqlite): " + url);
    }

    private static String[] splitUserInfo(String userInfo) {
        if (userInfo == null) {
            return new String[] {"", ""};
        }
        int colon = userInfo.indexOf(':');
        String user = colon == -1 ? userInfo : userInfo.substring(0, colon);
        String password = colon == -1 ? "" : userInfo.substring(colon + 1);
        return new String[] {
            URLDecoder.decode(user, StandardCharsets.UTF_8),
            URLDecoder.decode(password, StandardCharsets.UTF_8),
        };
    }
}
