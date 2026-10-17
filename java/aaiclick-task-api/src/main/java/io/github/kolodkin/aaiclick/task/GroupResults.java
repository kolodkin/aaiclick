package io.github.kolodkin.aaiclick.task;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/** A ``group_results`` ref: the list of a group's results, by handle or committed id. */
@JsonPropertyOrder({"_aaiclick_type", "group_id"})
public final class GroupResults {

    @JsonProperty("_aaiclick_type")
    private final String type = "group_results";
    @JsonProperty("group_id")
    private final Object groupId;

    private GroupResults(Object groupId) {
        this.groupId = groupId;
    }

    public static GroupResults of(String handle) {
        return new GroupResults(handle);
    }

    public static GroupResults of(long committedGroupId) {
        return new GroupResults(committedGroupId);
    }
}
