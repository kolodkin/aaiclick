package io.github.kolodkin.aaiclick.task;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Return value that spawns children: ``data`` is this task's result (a plain
 * value, or a child's {@link Task#ref()} so consumers wait for and get that
 * child's value), ``tasks`` are scheduled by the host.
 */
@JsonPropertyOrder({"_aaiclick_type", "data", "tasks"})
public final class TaskResult {

    @JsonProperty("_aaiclick_type")
    private final String type = "task_result";
    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final Object data;
    @JsonProperty
    private final List<DagNode> tasks;

    private TaskResult(Object data, List<DagNode> tasks) {
        this.data = data;
        this.tasks = tasks;
    }

    public static TaskResult of(Object data, DagNode... tasks) {
        return new TaskResult(data, List.of(tasks));
    }

    /** Spawn children with no result of its own. */
    public static TaskResult spawn(DagNode... tasks) {
        return new TaskResult(null, List.of(tasks));
    }
}
