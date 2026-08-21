package io.github.kolodkin.aaiclick.task;

import java.lang.reflect.Method;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KwargsBinderTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String SAMPLE = "io.github.kolodkin.aaiclick.task.testsupport.SampleTasks";

    private static ObjectNode kwargs(String json) throws Exception {
        return (ObjectNode) MAPPER.readTree(json);
    }

    @Test
    void bindsByNameWithTypeConversion() throws Exception {
        Method m = TaskRegistry.resolve(SAMPLE + "#aggregate");
        Object[] args = KwargsBinder.bind(m, kwargs("{\"window\": 7, \"date\": \"2026-08-20\"}"), MAPPER);
        assertArrayEquals(new Object[] {"2026-08-20", 7}, args);
    }

    @Test
    void bindsGenericCollections() throws Exception {
        Method m = TaskRegistry.resolve(SAMPLE + "#sum");
        Object[] args = KwargsBinder.bind(m, kwargs("{\"values\": [1, 2.5, 3]}"), MAPPER);
        assertEquals(List.of(1.0, 2.5, 3.0), args[0]);
    }

    @Test
    void missingKwargFails() throws Exception {
        Method m = TaskRegistry.resolve(SAMPLE + "#aggregate");
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class, () -> KwargsBinder.bind(m, kwargs("{\"date\": \"d\"}"), MAPPER));
        assertTrue(e.getMessage().contains("window"));
    }

    @Test
    void unexpectedKwargFails() throws Exception {
        Method m = TaskRegistry.resolve(SAMPLE + "#aggregate");
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> KwargsBinder.bind(m, kwargs("{\"date\": \"d\", \"window\": 1, \"typo\": true}"), MAPPER));
        assertTrue(e.getMessage().contains("typo"));
    }
}
