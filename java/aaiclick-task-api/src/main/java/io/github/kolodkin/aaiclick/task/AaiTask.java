package io.github.kolodkin.aaiclick.task;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a public static method as an aaiclick task entrypoint.
 *
 * <p>A {@code jvm} task's {@code entrypoint} is the declaring class name
 * ({@code com.example.Pipeline}), or {@code com.example.Pipeline#method} when
 * the class annotates more than one method. The {@link AaiTaskShim} bootstrap
 * binds the task's JSON kwargs to the method's parameters by name — compile
 * task classes with {@code -parameters}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface AaiTask {
}
