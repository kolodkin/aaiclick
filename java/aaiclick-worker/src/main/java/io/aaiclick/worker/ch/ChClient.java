package io.aaiclick.worker.ch;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.aaiclick.worker.config.WorkerConfig;

/** Minimal ClickHouse HTTP client: scalar queries and JSONEachRow inserts.
 *
 * The worker's only ClickHouse touchpoints are generateSnowflakeID() and
 * task_logs inserts — no Object/View support by design.
 */
public class ChClient {

    private final HttpClient http = HttpClient.newHttpClient();
    private final String baseUrl;
    private final String user;
    private final String password;
    private final String database;

    public ChClient(WorkerConfig cfg) {
        this.baseUrl = cfg.chHttpUrl();
        this.user = cfg.chUser();
        this.password = cfg.chPassword();
        this.database = cfg.chDatabase();
    }

    public String query(String sql) {
        return post(sql, "");
    }

    public void insertJsonEachRow(String table, List<ObjectNode> rows) {
        if (rows.isEmpty()) {
            return;
        }
        String body = rows.stream().map(ObjectNode::toString).collect(Collectors.joining("\n"));
        post("INSERT INTO " + table + " FORMAT JSONEachRow", body);
    }

    public long nextSnowflakeId() {
        return Long.parseLong(query("SELECT generateSnowflakeID()"));
    }

    private String post(String sql, String body) {
        String url = baseUrl + "/?database=" + URLEncoder.encode(database, StandardCharsets.UTF_8)
            + "&query=" + URLEncoder.encode(sql, StandardCharsets.UTF_8);
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url))
            .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
        if (!user.isEmpty()) {
            builder.header("X-ClickHouse-User", user).header("X-ClickHouse-Key", password);
        }
        try {
            HttpResponse<String> response = http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new ChException("ClickHouse HTTP " + response.statusCode() + ": " + response.body());
            }
            String text = response.body();
            return text.endsWith("\n") ? text.substring(0, text.length() - 1) : text;
        } catch (IOException e) {
            throw new ChException("ClickHouse request failed: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ChException("ClickHouse request interrupted", e);
        }
    }

    public static class ChException extends RuntimeException {
        public ChException(String message) { super(message); }
        public ChException(String message, Throwable cause) { super(message, cause); }
    }
}
