package io.aaiclick.worker.logs;

import java.time.Instant;

/** One captured output line; level defaults mirror aaiclick print capture:
 *  stdout -> INFO, stderr -> WARNING (ERROR is reserved for real log records). */
public record LogLine(String stream, String level, String text, Instant createdAt) {

    public static LogLine stdout(String text) {
        return new LogLine("stdout", "INFO", text, Instant.now());
    }

    public static LogLine stderr(String text) {
        return new LogLine("stderr", "WARNING", text, Instant.now());
    }
}
