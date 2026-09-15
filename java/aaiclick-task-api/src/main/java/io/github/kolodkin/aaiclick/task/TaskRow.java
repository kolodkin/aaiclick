package io.github.kolodkin.aaiclick.task;

/** The claimed task's row subset the shim needs. ``kwargsJson`` is the raw
 * JSON column text (may be null). */
public record TaskRow(long id, long jobId, String entrypoint, String kwargsJson) {
}
