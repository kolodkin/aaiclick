package io.aaiclick.worker.exec;

/** Outcome of one shell task run; COMPLETED iff exit code 0. */
public record ShellResult(Outcome outcome, int exitCode) {

    public enum Outcome { COMPLETED, FAILED, TIMEOUT, ABORTED }
}
