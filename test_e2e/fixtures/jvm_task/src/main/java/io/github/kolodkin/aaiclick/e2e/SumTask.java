package io.github.kolodkin.aaiclick.e2e;

import java.util.List;
import java.util.Map;

import io.github.kolodkin.aaiclick.task.AaiTask;

/** The jvm leg of the docker-runner e2e chain: sums the list an upstream
 * Python task produced and returns a plain map a downstream Python task
 * consumes. */
public final class SumTask {

    private SumTask() {
    }

    @AaiTask
    public static Map<String, Object> sum(List<Double> values) {
        double total = values.stream().mapToDouble(Double::doubleValue).sum();
        return Map.of("total", total, "count", values.size());
    }
}
