package io.aaiclick.worker.logs;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.aaiclick.worker.ch.ChClient;

/** Buffers task output and writes it to ClickHouse task_logs in batches.
 *
 * seq is strictly increasing per run_id across flushes (the offset survives
 * the batch), matching _SinkFlusher in aaiclick/orchestration/logging.py.
 * Thread-safe add(); flush()/close() are called from the runner's timer.
 */
public class LogFlusher {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ChClient ch;
    private final long taskId;
    private final long jobId;
    private final long runId;
    private final List<LogLine> buffer = new ArrayList<>();
    private long seqOffset;

    public LogFlusher(ChClient ch, long taskId, long jobId, long runId) {
        this.ch = ch;
        this.taskId = taskId;
        this.jobId = jobId;
        this.runId = runId;
    }

    public synchronized void add(LogLine line) {
        buffer.add(line);
    }

    public void flush() {
        List<LogLine> batch;
        long offset;
        synchronized (this) {
            if (buffer.isEmpty()) {
                return;
            }
            batch = new ArrayList<>(buffer);
            buffer.clear();
            offset = seqOffset;
            seqOffset += batch.size();
        }
        List<ObjectNode> rows = new ArrayList<>(batch.size());
        for (int i = 0; i < batch.size(); i++) {
            LogLine line = batch.get(i);
            ObjectNode row = MAPPER.createObjectNode();
            row.put("task_id", taskId);
            row.put("job_id", jobId);
            row.put("run_id", runId);
            row.put("seq", offset + i);
            row.put("stream", line.stream());
            row.put("level", line.level());
            row.put("line", line.text());
            row.put("created_at", line.createdAt().toEpochMilli() / 1000.0);
            rows.add(row);
        }
        ch.insertJsonEachRow("task_logs", rows);
    }

    public void close() {
        flush();
    }
}
