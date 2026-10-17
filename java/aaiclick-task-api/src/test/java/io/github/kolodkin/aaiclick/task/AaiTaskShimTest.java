package io.github.kolodkin.aaiclick.task;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** End-to-end shim runs on the file data plane: ``in.json`` in, ``out.json``
 * out — the same contract every shell task speaks. */
class AaiTaskShimTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String SAMPLE = "io.github.kolodkin.aaiclick.task.testsupport.SampleTasks";

    @TempDir
    Path io;

    private int runShim(String entrypoint) {
        return AaiTaskShim.run(new String[] {entrypoint}, Map.of(AaiTaskShim.IO_DIR_ENV, io.toString()));
    }

    private void writeIn(String json) throws IOException {
        Files.writeString(io.resolve("in.json"), json);
    }

    private JsonNode readOut() throws IOException {
        return MAPPER.readTree(Files.readString(io.resolve("out.json")));
    }

    @Test
    void successWritesTheReturnValueAsOutJson() throws Exception {
        writeIn("{\"date\": \"2026-08-20\", \"window\": 7}");

        assertEquals(0, runShim(SAMPLE + "#aggregate"));

        assertEquals(MAPPER.readTree("{\"date\": \"2026-08-20\", \"window\": 14}"), readOut());
    }

    @Test
    void taggedMapIsWrittenAsIs() throws Exception {
        writeIn("{\"n\": 2}");

        assertEquals(0, runShim(SAMPLE + "#spawn"));

        assertEquals(MAPPER.readTree("{\"_aaiclick_type\": \"task_result\", \"data\": 2, \"tasks\": []}"), readOut());
    }

    @Test
    void voidResultWritesNoOutJson() throws Exception {
        writeIn("{\"ignored\": \"x\"}");

        assertEquals(0, runShim(SAMPLE + "#sideEffectOnly"));

        assertFalse(Files.exists(io.resolve("out.json")));
    }

    @Test
    void taskExceptionReturnsOneAndWritesNothing() throws Exception {
        writeIn("{\"message\": \"boom\"}");

        assertEquals(1, runShim(SAMPLE + "#explode"));

        assertFalse(Files.exists(io.resolve("out.json")));
    }

    @Test
    void kwargNamedLikeTheTagStaysAParameter() throws Exception {
        writeIn("{\"_aaiclick_type\": \"kept\"}");

        assertEquals(0, runShim(SAMPLE + "#tagNamed"));

        assertEquals(MAPPER.readTree("\"kept\""), readOut());
    }

    @Test
    void missingInJsonFails() {
        assertEquals(1, runShim(SAMPLE + "#aggregate"));
        assertTrue(Files.notExists(io.resolve("out.json")));
    }
}
