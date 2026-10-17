package io.github.kolodkin.aaiclick.task;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/** A ``group`` of child tasks; {@link #results()} hands a sibling the list of their results. */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"_aaiclick_type", "id", "name", "tasks"})
public final class Group implements DagNode {

    @JsonProperty("_aaiclick_type")
    private final String type = "group";
    @JsonProperty
    private final String id;
    @JsonProperty
    private String name;
    @JsonProperty
    private final List<Task> tasks;

    private Group(String id, List<Task> tasks) {
        this.id = id;
        this.tasks = tasks;
    }

    public static Group of(String id, Task... tasks) {
        return new Group(id, List.of(tasks));
    }

    public Group name(String name) {
        this.name = name;
        return this;
    }

    /** Only a {@link Task#module} sibling may take this: shell and jvm kwargs carry plain values and upstream refs only. */
    public GroupResults results() {
        if (id == null) {
            throw new IllegalStateException("a group referenced by a sibling needs an id");
        }
        return new GroupResults(id);
    }
}
