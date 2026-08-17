package io.aaiclick.worker.exec;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

import io.aaiclick.worker.logs.LogLine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShellRunnerTest {

    private final CopyOnWriteArrayList<LogLine> lines = new CopyOnWriteArrayList<>();

    @Test
    void successCapturesStdout() {
        ShellResult result = new ShellRunner().run(
            List.of("sh", "-c", "echo out; echo err >&2"), null, null, () -> false, lines::add);
        assertEquals(ShellResult.Outcome.COMPLETED, result.outcome());
        assertEquals(0, result.exitCode());
        assertTrue(lines.stream().anyMatch(l -> l.stream().equals("stdout") && l.text().equals("out")));
        assertTrue(lines.stream().anyMatch(l -> l.stream().equals("stderr") && l.text().equals("err")));
    }

    @Test
    void nonZeroExitIsFailed() {
        ShellResult result = new ShellRunner().run(
            List.of("sh", "-c", "exit 3"), null, null, () -> false, lines::add);
        assertEquals(ShellResult.Outcome.FAILED, result.outcome());
        assertEquals(3, result.exitCode());
    }

    @Test
    void commandEnvOverlaysWorkerEnv() {
        ShellResult result = new ShellRunner().run(
            List.of("sh", "-c", "test \"$MY_MARKER\" = overlay && test -n \"$PATH\""),
            Map.of("MY_MARKER", "overlay"), null, () -> false, lines::add);
        assertEquals(ShellResult.Outcome.COMPLETED, result.outcome());
    }

    @Test
    void timeoutKillsProcess() {
        long start = System.nanoTime();
        ShellResult result = new ShellRunner().run(
            List.of("sleep", "30"), null, 1.0, () -> false, lines::add);
        assertEquals(ShellResult.Outcome.TIMEOUT, result.outcome());
        assertTrue((System.nanoTime() - start) / 1_000_000_000.0 < 15);
    }

    @Test
    void abortKillsProcess() {
        long start = System.nanoTime();
        ShellResult result = new ShellRunner().run(
            List.of("sleep", "30"), null, null, () -> true, lines::add);
        assertEquals(ShellResult.Outcome.ABORTED, result.outcome());
        assertTrue((System.nanoTime() - start) / 1_000_000_000.0 < 15);
    }
}
