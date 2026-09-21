package io.github.kolodkin.aaiclick.task;

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
        int schemeEnd = url.indexOf("://");
        if (schemeEnd == -1) {
            throw new IllegalArgumentException("Unparseable AAICLICK_SQL_URL: " + url);
        }
        String rest = url.substring(schemeEnd + 3);
        if (url.startsWith("postgresql")) {
            // The JDBC URL is the SQLAlchemy URL minus its dialect suffix and
            // credentials: host, port, path and query pass through, and the
            // driver supplies the default port. Not java.net.URI, which
            // rejects underscored hosts and characters SQLAlchemy accepts.
            // Credentials end at the first '@' (SQLAlchemy's password may hold
            // anything but '@'; its username nothing of ':' or '/').
            int at = rest.indexOf('@');
            boolean hasCredentials = at != -1 && !rest.substring(0, at).split(":", 2)[0].contains("/");
            String[] userInfo = splitUserInfo(hasCredentials ? rest.substring(0, at) : null);
            String hostPathQuery = hasCredentials ? rest.substring(at + 1) : rest;
            // asyncpg's ssl=<mode> and pgjdbc's sslmode=<mode> share libpq's
            // vocabulary; pgjdbc's own `ssl` is a boolean that reads any mode
            // as "prefer".
            String jdbc = "jdbc:postgresql://" + hostPathQuery.replaceFirst("([?&])ssl=", "$1sslmode=");
            return new SqlConfig(Dialect.POSTGRES, jdbc, userInfo[0], userInfo[1]);
        }
        if (url.startsWith("sqlite")) {
            // sqlite:///rel.db → "rel.db"; sqlite:////abs/p.db → "/abs/p.db"
            String path = rest.startsWith("/") ? rest.substring(1) : rest;
            return new SqlConfig(Dialect.SQLITE, "jdbc:sqlite:" + path, "", "");
        }
        throw new IllegalArgumentException(
            "Unsupported AAICLICK_SQL_URL scheme (expected postgresql or sqlite): " + url);
    }

    /** Split raw userinfo on its first {@code :}, then percent-decode each side. */
    private static String[] splitUserInfo(String rawUserInfo) {
        if (rawUserInfo == null) {
            return new String[] {"", ""};
        }
        int colon = rawUserInfo.indexOf(':');
        String user = colon == -1 ? rawUserInfo : rawUserInfo.substring(0, colon);
        String password = colon == -1 ? "" : rawUserInfo.substring(colon + 1);
        return new String[] {percentDecode(user), percentDecode(password)};
    }

    /**
     * Percent-decoding as Python's {@code unquote} does it: a literal {@code +}
     * stays, and a {@code %} not followed by two hex digits stays verbatim.
     */
    private static String percentDecode(String s) {
        String escaped = s.replace("+", "%2B").replaceAll("%(?![0-9A-Fa-f]{2})", "%25");
        return URLDecoder.decode(escaped, StandardCharsets.UTF_8);
    }
}
