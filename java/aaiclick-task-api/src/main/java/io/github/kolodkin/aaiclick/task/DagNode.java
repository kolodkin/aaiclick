package io.github.kolodkin.aaiclick.task;

/** A child a task may return for the host to schedule: a {@link Task} or a {@link Group}. */
public sealed interface DagNode permits Task, Group {
}
