package io.github.kolodkin.aaiclick.task.testsupport;

import io.github.kolodkin.aaiclick.task.AaiTask;

/** Two annotated methods — entrypoints must disambiguate with {@code #method}. */
public final class MultiTasks {

    private MultiTasks() {
    }

    @AaiTask
    public static int first(int x) {
        return x + 1;
    }

    @AaiTask
    public static int second(int x) {
        return x + 2;
    }

    @AaiTask
    static int hidden(int x) {
        return x;
    }
}
