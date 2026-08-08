package io.aaiclick.worker;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.aaiclick.worker.ch.ChClient;
import io.aaiclick.worker.config.WorkerConfig;
import io.aaiclick.worker.db.ClaimedTask;
import io.aaiclick.worker.db.Db;
import io.aaiclick.worker.db.TaskRepo;
import io.aaiclick.worker.db.WorkerRepo;
import io.aaiclick.worker.exec.ShellResult;
import io.aaiclick.worker.exec.ShellRunner;
import io.aaiclick.worker.logs.LogFlusher;

/** Distributed-mode shell-task worker: claim, execute, report, repeat. */
public class Worker {

    private static final long POLL_INTERVAL_MS = 1000;
    private static final long LOG_FLUSH_INTERVAL_MS = 2000;

    private final WorkerConfig cfg;
    private final ChClient ch;
    private final WorkerRepo workers;
    private final TaskRepo tasks;
    private final ShellRunner runner = new ShellRunner();
    private final AtomicBoolean stopRequested = new AtomicBoolean(false);
    private long workerId;

    public Worker(WorkerConfig cfg) {
        this.cfg = cfg;
        this.ch = new ChClient(cfg);
        Db db = new Db(cfg);
        this.workers = new WorkerRepo(db);
        this.tasks = new TaskRepo(db);
    }

    public long workerId() {
        return workerId;
    }

    public void requestStop() {
        stopRequested.set(true);
    }

    public void runLoop(int maxTasks) throws SQLException, InterruptedException {
        workerId = ch.nextSnowflakeId();
        workers.register(workerId, hostname(), (int) ProcessHandle.current().pid());
        int completed = 0;
        try {
            while (!stopRequested.get() && (maxTasks <= 0 || completed < maxTasks)) {
                String status = workers.heartbeat(workerId);
                if ("STOPPING".equals(status)) {
                    break;
                }
                ClaimedTask task = tasks.claimNext(workerId);
                if (task == null) {
                    Thread.sleep(POLL_INTERVAL_MS);
                    continue;
                }
                runOne(task);
                completed++;
            }
        } finally {
            workers.markStopped(workerId);
        }
    }

    private void runOne(ClaimedTask task) throws SQLException {
        long runId = ch.nextSnowflakeId();
        tasks.startRun(task.id(), runId);
        LogFlusher flusher = new LogFlusher(ch, task.id(), task.jobId(), runId);
        ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "log-flush-" + task.id());
            t.setDaemon(true);
            return t;
        });
        timer.scheduleAtFixedRate(flusher::flush, LOG_FLUSH_INTERVAL_MS, LOG_FLUSH_INTERVAL_MS,
            TimeUnit.MILLISECONDS);
        try {
            ShellResult result = runner.run(
                task.command(), task.commandEnv(), cfg.taskTimeoutSeconds(),
                () -> {
                    try {
                        return tasks.isRunAborted(task.id(), task.runEpoch());
                    } catch (SQLException e) {
                        return false;  // transient DB error must not kill the child
                    }
                },
                flusher::add);
            switch (result.outcome()) {
                case COMPLETED -> {
                    if (tasks.complete(task.id(), task.runEpoch())) {
                        tasks.tryCompleteJob(task.jobId());
                        workers.bumpCompleted(workerId);
                    }
                }
                case FAILED -> fail(task, "Command failed with exit code " + result.exitCode());
                case TIMEOUT -> fail(task, "Task timed out after " + cfg.taskTimeoutSeconds() + " seconds");
                case ABORTED -> { /* cancel_job / clear_task owns the task row */ }
            }
        } finally {
            timer.shutdown();
            flusher.close();
        }
    }

    private void fail(ClaimedTask task, String error) throws SQLException {
        if (tasks.failPendingCleanup(task.id(), task.runEpoch(), error)) {
            workers.bumpFailed(workerId);
        }
    }

    private static String hostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "unknown";
        }
    }

    public static void main(String[] args) throws Exception {
        int maxTasks = 0;
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals("--max-tasks")) {
                maxTasks = Integer.parseInt(args[i + 1]);
            }
        }
        Worker worker = new Worker(WorkerConfig.fromEnv(System.getenv()));
        Runtime.getRuntime().addShutdownHook(new Thread(worker::requestStop));
        worker.runLoop(maxTasks);
    }
}
