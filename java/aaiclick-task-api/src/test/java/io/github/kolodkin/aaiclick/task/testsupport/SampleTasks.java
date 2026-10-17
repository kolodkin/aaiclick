package io.github.kolodkin.aaiclick.task.testsupport;

import java.util.Arrays;
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
    public static int total(int[] counts) {
        return Arrays.stream(counts).sum();
    }

    @AaiTask
    public static void sideEffectOnly(String ignored) {
    }

    @AaiTask
    public static String tagNamed(String _aaiclick_type) {
        return _aaiclick_type;
    }

    /** The JSON form of a dynamic DAG: written to out.json as is. */
    @AaiTask
    public static Map<String, Object> spawn(int n) {
        return Map.of("_aaiclick_type", "task_result", "data", n, "tasks", List.of());
    }

    @AaiTask
    public static String explode(String message) {
        throw new IllegalStateException(message);
    }
}
