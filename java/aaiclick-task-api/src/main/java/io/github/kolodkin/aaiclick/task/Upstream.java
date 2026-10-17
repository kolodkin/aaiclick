package io.github.kolodkin.aaiclick.task;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/** An ``upstream`` ref to a sibling task; built by {@link Task#ref()}. */
@JsonPropertyOrder({"_aaiclick_type", "task_id"})
public final class Upstream {

    @JsonProperty("_aaiclick_type")
    private final String type = "upstream";
    @JsonProperty("task_id")
    private final String taskId;

    Upstream(String handle) {
        this.taskId = handle;
    }
}
