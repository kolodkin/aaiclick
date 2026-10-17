package io.github.kolodkin.aaiclick.task.testsupport;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import io.github.kolodkin.aaiclick.task.AaiTask;
import io.github.kolodkin.aaiclick.task.Group;
import io.github.kolodkin.aaiclick.task.Task;
import io.github.kolodkin.aaiclick.task.TaskResult;

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

    /** The same DAG shape built from the typed classes: two workers, a sum, a group. */
    @AaiTask
    public static TaskResult fanOut(String image) {
        Task w1 = Task.shell("w1", "node", "worker.js").kwarg("n", 1);
        Task w2 = Task.shell("w2", "node", "worker.js").kwarg("n", 2).maxRetries(1);
        Task fin = Task.jvm("fin", "com.example.Sum").kwarg("parts", List.of(w1.ref(), w2.ref())).image(image);
        Group g = Group.of("g", Task.module(null, "pkg.mod.step"));
        Task report = Task.jvm("report", "com.example.Report").kwarg("steps", g.results()).dependsOn(fin);
        return TaskResult.of(fin.ref(), w1, w2, fin, g, report);
    }

    @AaiTask
    public static String explode(String message) {
        throw new IllegalStateException(message);
    }
}
