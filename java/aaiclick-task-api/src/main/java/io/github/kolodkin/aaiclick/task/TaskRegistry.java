package io.github.kolodkin.aaiclick.task;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/** Resolves a ``jvm`` task's entrypoint string to its {@link AaiTask} method:
 * ``com.example.Pipeline``, or ``com.example.Pipeline#method`` when the class
 * annotates more than one method. */
public final class TaskRegistry {

    private TaskRegistry() {
    }

    public static Method resolve(String entrypoint) {
        String className = entrypoint;
        String methodName = null;
        int hash = entrypoint.indexOf('#');
        if (hash >= 0) {
            className = entrypoint.substring(0, hash);
            methodName = entrypoint.substring(hash + 1);
        }

        Class<?> cls;
        try {
            cls = Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Task class not found on the classpath: " + className, e);
        }

        List<Method> annotated = Arrays.stream(cls.getDeclaredMethods())
            .filter(m -> m.isAnnotationPresent(AaiTask.class))
            .sorted(Comparator.comparing(Method::getName))
            .toList();
        if (annotated.isEmpty()) {
            throw new IllegalArgumentException("No @AaiTask method on " + className);
        }

        final String wanted = methodName;
        List<Method> matches = wanted == null
            ? annotated
            : annotated.stream().filter(m -> m.getName().equals(wanted)).toList();
        if (matches.isEmpty()) {
            throw new IllegalArgumentException("No @AaiTask method named " + wanted + " on " + className);
        }
        if (matches.size() > 1) {
            String names = String.join(", ", matches.stream().map(Method::getName).distinct().toList());
            throw new IllegalArgumentException(wanted == null
                ? "Multiple @AaiTask methods on " + className + " (" + names
                    + ") — disambiguate the entrypoint as " + className + "#method"
                : "Overloaded @AaiTask methods named " + wanted + " on " + className
                    + " — overloads are not supported");
        }

        Method method = matches.get(0);
        if (!Modifier.isStatic(method.getModifiers()) || !Modifier.isPublic(method.getModifiers())) {
            throw new IllegalArgumentException("@AaiTask method must be public static: " + method);
        }
        return method;
    }
}
