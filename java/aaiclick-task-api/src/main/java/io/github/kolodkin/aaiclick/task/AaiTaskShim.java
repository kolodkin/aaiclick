package io.github.kolodkin.aaiclick.task;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Container-side bootstrap for ``jvm`` entry-type tasks — the JVM mirror of
 * the Python ``remote_result`` shim. The runner invokes the image's
 * ``ENTRYPOINT`` (this class) with ``--task-id N --run-epoch M``; the shim
 * loads the task row, resolves and binds kwargs to the {@link AaiTask}
 * method, invokes it, and writes the ``remote_task_results`` row the host
 * worker reads back. Exit code 0 on success, 1 on failure — the row, not the
 * exit code, carries the error detail.
 *
 * <pre>
 * ENTRYPOINT ["java", "-cp", "/app/app.jar:/app/libs/*",
 *             "io.github.kolodkin.aaiclick.task.AaiTaskShim"]
 * </pre>
 */
public final class AaiTaskShim {

    private AaiTaskShim() {
    }

    public static void main(String[] args) {
        System.exit(run(args, System.getenv()));
    }

    static int run(String[] args, Map<String, String> env) {
        long taskId = requireLongArg(args, "--task-id");
        long runEpoch = requireLongArg(args, "--run-epoch");
        try {
            SqlConfig cfg = SqlConfig.fromEnv(env);
            try (Connection conn = new Db(cfg).connect()) {
                TaskStore store = new TaskStore(conn, cfg.dialect());
                boolean success = false;
                String resultRef = null;
                String error = null;
                try {
                    resultRef = executeTask(store, taskId);
                    success = true;
                } catch (InvocationTargetException e) {
                    // The task method itself threw — report its cause.
                    Throwable cause = e.getCause() != null ? e.getCause() : e;
                    error = format(cause);
                } catch (Throwable t) {
                    error = format(t);
                }
                store.writeResult(taskId, runEpoch, success, resultRef, error);
                return success ? 0 : 1;
            }
        } catch (Exception e) {
            // No result row could be written — the host worker synthesizes
            // "exited with code N but wrote no result row".
            System.err.println("aaiclick task shim failed before writing a result row: " + format(e));
            return 1;
        }
    }

    /** Load, resolve, bind, invoke; returns the serialized result_ref JSON
     * (``{"native_value": ...}``) or null for a void/null result. */
    private static String executeTask(TaskStore store, long taskId) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        TaskRow row = store.loadTask(taskId);

        JsonNode rawKwargs = row.kwargsJson() == null || row.kwargsJson().isEmpty()
            ? mapper.createObjectNode()
            : mapper.readTree(row.kwargsJson());
        if (!rawKwargs.isObject()) {
            throw new IllegalStateException("Task " + taskId + " kwargs is not a JSON object");
        }
        JsonNode resolved = new KwargsResolver(store, mapper).resolve(rawKwargs);

        Method method = TaskRegistry.resolve(row.entrypoint());
        Object[] bound = KwargsBinder.bind(method, (ObjectNode) resolved, mapper);
        Object result = method.invoke(null, bound);

        if (result == null || method.getReturnType() == void.class) {
            return null;
        }
        ObjectNode ref = mapper.createObjectNode();
        ref.set(KwargsResolver.NATIVE_VALUE, mapper.valueToTree(result));
        return mapper.writeValueAsString(ref);
    }

    private static long requireLongArg(String[] args, String flag) {
        for (int i = 0; i < args.length - 1; i++) {
            if (flag.equals(args[i])) {
                return Long.parseLong(args[i + 1]);
            }
        }
        throw new IllegalArgumentException("Missing required argument: " + flag + " <value>");
    }

    private static String format(Throwable t) {
        return t.getClass().getSimpleName() + ": " + t.getMessage();
    }
}
