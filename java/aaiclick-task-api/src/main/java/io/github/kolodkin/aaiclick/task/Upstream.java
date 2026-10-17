package io.github.kolodkin.aaiclick.task;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/** An ``upstream`` ref: a sibling's handle ({@link Task#ref()}) or a committed task id. */
@JsonPropertyOrder({"_aaiclick_type", "task_id"})
public final class Upstream {

    @JsonProperty("_aaiclick_type")
    private final String type = "upstream";
    @JsonProperty("task_id")
    private final Object taskId;

    private Upstream(Object taskId) {
        this.taskId = taskId;
    }

    public static Upstream of(String handle) {
        return new Upstream(handle);
    }

    public static Upstream of(long committedTaskId) {
        return new Upstream(committedTaskId);
    }
}
