package io.aaiclick.worker.db;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** A SQL text with :named parameters, converted once to JDBC positional form.
 *
 * Values are always bound through PreparedStatement — the conversion rewrites
 * only the SQL text, never splices values in. The scanner skips Postgres
 * ``::`` casts, single-quoted literals, and ``--`` / block comments (comments
 * may mention parameter names and contain apostrophes).
 */
public record NamedParamSql(String jdbcSql, List<String> paramOrder) {

    public static NamedParamSql fromResource(String path) {
        try (InputStream in = NamedParamSql.class.getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException("SQL resource not found on classpath: " + path);
            }
            return parse(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read SQL resource: " + path, e);
        }
    }

    public static NamedParamSql parse(String sql) {
        StringBuilder out = new StringBuilder(sql.length());
        List<String> order = new ArrayList<>();
        boolean inString = false;
        boolean inLineComment = false;
        boolean inBlockComment = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (inLineComment) {
                inLineComment = c != '\n';
                out.append(c);
                continue;
            }
            if (inBlockComment) {
                if (c == '*' && i + 1 < sql.length() && sql.charAt(i + 1) == '/') {
                    inBlockComment = false;
                    out.append("*/");
                    i++;
                    continue;
                }
                out.append(c);
                continue;
            }
            if (!inString && c == '-' && i + 1 < sql.length() && sql.charAt(i + 1) == '-') {
                inLineComment = true;
                out.append(c);
                continue;
            }
            if (!inString && c == '/' && i + 1 < sql.length() && sql.charAt(i + 1) == '*') {
                inBlockComment = true;
                out.append(c);
                continue;
            }
            if (c == '\'') {
                inString = !inString;
                out.append(c);
                continue;
            }
            if (inString || c != ':') {
                out.append(c);
                continue;
            }
            // A ':' outside a string literal: cast, or named parameter?
            boolean castBefore = i > 0 && sql.charAt(i - 1) == ':';
            boolean castAfter = i + 1 < sql.length() && sql.charAt(i + 1) == ':';
            if (castBefore || castAfter) {
                out.append(c);
                continue;
            }
            int end = i + 1;
            while (end < sql.length() && (Character.isLetterOrDigit(sql.charAt(end)) || sql.charAt(end) == '_')) {
                end++;
            }
            if (end == i + 1) {
                out.append(c);
                continue;
            }
            order.add(sql.substring(i + 1, end));
            out.append('?');
            i = end - 1;
        }
        return new NamedParamSql(out.toString(), List.copyOf(order));
    }
}
