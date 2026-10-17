package io.github.kolodkin.aaiclick.task;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/** A ``group_results`` ref to a sibling group; built by {@link Group#results()}. */
@JsonPropertyOrder({"_aaiclick_type", "group_id"})
public final class GroupResults {

    @JsonProperty("_aaiclick_type")
    private final String type = "group_results";
    @JsonProperty("group_id")
    private final String groupId;

    GroupResults(String handle) {
        this.groupId = handle;
    }
}
