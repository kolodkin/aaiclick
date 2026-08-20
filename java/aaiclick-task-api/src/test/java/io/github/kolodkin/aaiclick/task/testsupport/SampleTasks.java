package io.github.kolodkin.aaiclick.task.testsupport;

import java.util.List;
import java.util.Map;

import io.github.kolodkin.aaiclick.task.AaiTask;

/** Annotated fixture methods for the registry, binder, and shim tests. */
public final class SampleTasks {

    private SampleTasks() {
    }

    @AaiTask
    public static Map<String, Object> aggregate(String date, int window) {
        return Map.of("date", date, "window", window * 2);
    }

    @AaiTask
    public static double sum(List<Double> values) {
        return values.stream().mapToDouble(Double::doubleValue).sum();
    }

    @AaiTask
    public static void sideEffectOnly(String ignored) {
    }

    @AaiTask
    public static String explode(String message) {
        throw new IllegalStateException(message);
    }
}
