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
            String[] userInfo = splitUserInfo(parsed.getRawUserInfo());
            // The query string rides along verbatim (e.g. ssl=require).
            String query = parsed.getRawQuery() == null ? "" : "?" + parsed.getRawQuery();
            String jdbc = "jdbc:postgresql://" + hostPort(parsed) + parsed.getRawPath() + query;
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

    /**
     * {@code host:port} from the raw authority. {@link URI#getHost()} is null for
     * names that are not RFC 2396 hostnames (an underscore, as in a compose
     * service {@code postgres_db}), which the driver resolves fine.
     */
    private static String hostPort(URI parsed) {
        String authority = parsed.getRawAuthority();
        if (authority == null) {
            throw new IllegalArgumentException("postgresql URL has no host: " + parsed);
        }
        String hostPort = authority.substring(authority.lastIndexOf('@') + 1);
        int portSep = hostPort.lastIndexOf(':');
        boolean hasPort = portSep > hostPort.lastIndexOf(']');
        return hasPort ? hostPort : hostPort + ":5432";
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
     * Percent-decoding only: {@link URLDecoder} is form decoding, which turns a
     * literal {@code +} into a space, unlike SQLAlchemy's {@code unquote}.
     */
    private static String percentDecode(String s) {
        return URLDecoder.decode(s.replace("+", "%2B"), StandardCharsets.UTF_8);
    }
}
