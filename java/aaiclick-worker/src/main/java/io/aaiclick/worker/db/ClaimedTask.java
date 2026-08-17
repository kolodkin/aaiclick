package io.aaiclick.worker.db;

import java.util.List;
import java.util.Map;

/** The subset of a claimed tasks row the shell runner needs. */
public record ClaimedTask(
    long id,
    long jobId,
    String name,
    List<String> command,
    Map<String, String> commandEnv,
    long runEpoch
) {}
