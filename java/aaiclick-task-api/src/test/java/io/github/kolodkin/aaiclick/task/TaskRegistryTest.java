package io.github.kolodkin.aaiclick.task;

import java.lang.reflect.Method;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskRegistryTest {

    private static final String MULTI = "io.github.kolodkin.aaiclick.task.testsupport.MultiTasks";

    @Test
    void resolvesMethodByHashSuffix() {
        Method m = TaskRegistry.resolve(MULTI + "#first");
        assertEquals("first", m.getName());
    }

    @Test
    void bareClassNameWithMultipleTasksNeedsDisambiguation() {
        IllegalArgumentException e =
            assertThrows(IllegalArgumentException.class, () -> TaskRegistry.resolve(MULTI));
        assertTrue(e.getMessage().contains("#method"));
    }

    @Test
    void unknownClassAndUnknownMethodFail() {
        assertThrows(IllegalArgumentException.class, () -> TaskRegistry.resolve("com.example.Nope"));
        assertThrows(IllegalArgumentException.class, () -> TaskRegistry.resolve(MULTI + "#nope"));
    }

    @Test
    void classWithoutAnnotatedMethodsFails() {
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class, () -> TaskRegistry.resolve("java.lang.String"));
        assertTrue(e.getMessage().contains("@AaiTask"));
    }

    @Test
    void nonPublicAnnotatedMethodFails() {
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class, () -> TaskRegistry.resolve(MULTI + "#hidden"));
        assertTrue(e.getMessage().contains("public static"));
    }
}
