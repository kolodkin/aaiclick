package io.aaiclick.worker.exec;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;

import io.aaiclick.worker.logs.LogLine;

/** Runs a shell task argv as a child process.
 *
 * Subprocess-runner semantics: the child inherits the worker's env with
 * command_env overlaid (no isolation boundary on this runner). The abort
 * check is the cancellation monitor — polled every POLL_INTERVAL_MS, it
 * covers cancel_job and clear_task epoch bumps.
 */
public class ShellRunner {

    private static final long POLL_INTERVAL_MS = 1000;
    private static final long KILL_GRACE_MS = 5000;

    public ShellResult run(
            List<String> command,
            Map<String, String> commandEnv,
            Double timeoutSeconds,
            BooleanSupplier abortCheck,
            Consumer<LogLine> sink) {
        ProcessBuilder builder = new ProcessBuilder(command);
        if (commandEnv != null) {
            builder.environment().putAll(commandEnv);
        }
        Process process;
        try {
            process = builder.start();
        } catch (IOException e) {
            sink.accept(LogLine.stderr("Failed to launch command: " + e.getMessage()));
            return new ShellResult(ShellResult.Outcome.FAILED, -1);
        }

        Thread stdoutPump = pump(process.getInputStream(), LogLine::stdout, sink);
        Thread stderrPump = pump(process.getErrorStream(), LogLine::stderr, sink);

        Instant deadline = timeoutSeconds == null
            ? null : Instant.now().plus(Duration.ofMillis((long) (timeoutSeconds * 1000)));
        try {
            while (true) {
                if (process.waitFor(POLL_INTERVAL_MS, TimeUnit.MILLISECONDS)) {
                    joinPumps(stdoutPump, stderrPump);
                    int exit = process.exitValue();
                    return new ShellResult(
                        exit == 0 ? ShellResult.Outcome.COMPLETED : ShellResult.Outcome.FAILED, exit);
                }
                if (abortCheck.getAsBoolean()) {
                    kill(process);
                    joinPumps(stdoutPump, stderrPump);
                    return new ShellResult(ShellResult.Outcome.ABORTED, -1);
                }
                if (deadline != null && Instant.now().isAfter(deadline)) {
                    kill(process);
                    joinPumps(stdoutPump, stderrPump);
                    return new ShellResult(ShellResult.Outcome.TIMEOUT, -1);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
            return new ShellResult(ShellResult.Outcome.ABORTED, -1);
        }
    }

    private static Thread pump(java.io.InputStream stream, Function<String, LogLine> factory,
            Consumer<LogLine> sink) {
        Thread thread = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(
                     new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    sink.accept(factory.apply(line));
                }
            } catch (IOException ignored) {
                // stream closes when the process dies — expected on kill
            }
        });
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private static void joinPumps(Thread stdoutPump, Thread stderrPump) throws InterruptedException {
        stdoutPump.join(KILL_GRACE_MS);
        stderrPump.join(KILL_GRACE_MS);
    }

    private static void kill(Process process) throws InterruptedException {
        process.destroy();
        if (!process.waitFor(KILL_GRACE_MS, TimeUnit.MILLISECONDS)) {
            process.destroyForcibly();
            process.waitFor(KILL_GRACE_MS, TimeUnit.MILLISECONDS);
        }
    }
}
