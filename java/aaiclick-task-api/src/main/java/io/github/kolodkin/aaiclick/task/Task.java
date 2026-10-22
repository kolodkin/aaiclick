package io.github.kolodkin.aaiclick.task;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * A child task to spawn — Jackson writes it as the Task Interface's ``task``
 * dict, whose fields are Python's ``create_task()`` parameters. Kwarg values
 * may be plain values, POJOs, or {@link Upstream} / {@link GroupResults}
 * refs to siblings in the same return value.
 *
 * <pre>
 * Task w1 = Task.jvm("w1", "com.example.Worker").kwarg("n", 1);
 * Task fin = Task.jvm("fin", "com.example.Sum").kwarg("parts", List.of(w1.ref()));
 * </pre>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"_aaiclick_type", "id", "entry_type", "entrypoint", "command"})
public final class Task implements DagNode {

    @JsonProperty("_aaiclick_type")
    private final String type = "task";
    @JsonProperty
    private final String id;
    @JsonProperty("entry_type")
    private final String entryType;
    @JsonProperty
    private final String entrypoint;
    @JsonProperty
    private final List<String> command;
    @JsonProperty
    private final Map<String, Object> kwargs = new LinkedHashMap<>();
    @JsonProperty
    private String name;
    @JsonProperty("max_retries")
    private Integer maxRetries;
    @JsonProperty("command_env")
    private Map<String, String> commandEnv;
    @JsonProperty
    private String image;
    @JsonProperty("depends_on")
    private List<String> dependsOn;

    private Task(String id, String entryType, String entrypoint, List<String> command) {
        this.id = id;
        this.entryType = entryType;
        this.entrypoint = entrypoint;
        this.command = command;
    }

    /** A jvm task: ``className`` (or ``Class#method``) resolved by this shim in the child's image. */
    public static Task jvm(String id, String className) {
        return new Task(id, "jvm", className, null);
    }

    /** A shell task running ``command`` as its argv. */
    public static Task shell(String id, String... command) {
        return new Task(id, "shell", null, List.of(command));
    }

    /** A Python task: ``entrypoint`` is a dotted function path. */
    public static Task module(String id, String entrypoint) {
        return new Task(id, "module", entrypoint, null);
    }

    public Task kwarg(String key, Object value) {
        kwargs.put(key, value);
        return this;
    }

    public Task kwargs(Map<String, ?> values) {
        kwargs.putAll(values);
        return this;
    }

    public Task name(String name) {
        this.name = name;
        return this;
    }

    public Task maxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    public Task env(String key, String value) {
        if (commandEnv == null) {
            commandEnv = new LinkedHashMap<>();
        }
        commandEnv.put(key, value);
        return this;
    }

    /** Run in this image; leave it unset to inherit the parent's. */
    public Task image(String image) {
        this.image = image;
        return this;
    }

    /** Run after these siblings without taking their results. */
    public Task dependsOn(Task... tasks) {
        if (dependsOn == null) {
            dependsOn = new ArrayList<>();
        }
        for (Task t : tasks) {
            dependsOn.add(t.handle());
        }
        return this;
    }

    /** An {@link Upstream} ref to this task, for a sibling's kwargs or {@link TaskResult} data. */
    public Upstream ref() {
        return new Upstream(handle());
    }

    private String handle() {
        if (id == null) {
            throw new IllegalStateException("a task referenced by a sibling needs an id");
        }
        return id;
    }
}
