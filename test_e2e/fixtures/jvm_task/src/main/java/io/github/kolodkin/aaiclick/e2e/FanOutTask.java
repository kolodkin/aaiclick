package io.github.kolodkin.aaiclick.e2e;

import java.util.List;
import java.util.Map;

import io.github.kolodkin.aaiclick.task.AaiTask;
import io.github.kolodkin.aaiclick.task.Task;
import io.github.kolodkin.aaiclick.task.TaskResult;

/** The jvm fan-out leg of the docker-runner e2e: returns a typed
 * {@link TaskResult} — two {@link SumTask} halves and a {@code combine} that
 * consumes both — so the host must register, wire and run a DAG the SDK built. */
public final class FanOutTask {

    private static final String SUM = "io.github.kolodkin.aaiclick.e2e.SumTask";
    private static final String COMBINE = "io.github.kolodkin.aaiclick.e2e.FanOutTask#combine";

    private FanOutTask() {
    }

    @AaiTask
    public static TaskResult fanOut(List<Double> values) {
        int mid = values.size() / 2;
        Task a = Task.jvm("a", SUM).kwarg("values", values.subList(0, mid));
        Task b = Task.jvm("b", SUM).kwarg("values", values.subList(mid, values.size()));
        Task fin = Task.jvm("fin", COMBINE).kwarg("parts", List.of(a.ref(), b.ref())).dependsOn(a);
        return TaskResult.of(fin.ref(), a, b, fin);
    }

    @AaiTask
    public static Map<String, Object> combine(List<Map<String, Number>> parts) {
        double total = parts.stream().mapToDouble(p -> p.get("total").doubleValue()).sum();
        int count = parts.stream().mapToInt(p -> p.get("count").intValue()).sum();
        return Map.of("total", total, "count", count);
    }
}
