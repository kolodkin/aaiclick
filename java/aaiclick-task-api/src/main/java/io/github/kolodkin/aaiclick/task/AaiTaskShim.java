package io.github.kolodkin.aaiclick.task;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Container-side bootstrap for ``jvm`` entry-type tasks on the aaiclick file
 * data plane. The runner invokes the image's ``ENTRYPOINT`` (this class) with
 * the task's class name as its only argument; the shim reads the kwargs from
 * ``$AAICLICK_IO_DIR/in.json``, binds them to the {@link AaiTask} method,
 * invokes it, and writes the return value to ``$AAICLICK_IO_DIR/out.json``.
 * No database: the host resolves kwargs before launch and interprets the
 * result after exit. Exit code 0 on success, 1 on failure (the cause goes to
 * stderr, which the host streams into the task's logs).
 *
 * <pre>
 * ENTRYPOINT ["java", "-cp", "/app/app.jar:/app/libs/*",
 *             "io.github.kolodkin.aaiclick.task.AaiTaskShim"]
 * </pre>
 */
public final class AaiTaskShim {

    static final String IO_DIR_ENV = "AAICLICK_IO_DIR";
    static final String IN_JSON = "in.json";
    static final String OUT_JSON = "out.json";

    private AaiTaskShim() {
    }

    public static void main(String[] args) {
        System.exit(run(args, System.getenv()));
    }

    static int run(String[] args, Map<String, String> env) {
        try {
            if (args.length != 1) {
                throw new IllegalArgumentException("usage: <task class>[#method] (got " + args.length + " arguments)");
            }
            String dir = env.get(IO_DIR_ENV);
            if (dir == null) {
                throw new IllegalStateException(IO_DIR_ENV + " is not set");
            }
            executeTask(args[0], Path.of(dir));
            return 0;
        } catch (InvocationTargetException e) {
            // The task method itself threw — report its cause.
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            System.err.println("aaiclick task failed: " + format(cause));
            return 1;
        } catch (Throwable t) {
            System.err.println("aaiclick task failed: " + format(t));
            return 1;
        }
    }

    /** Read, unwrap, bind, invoke, write. A ``Map`` carrying ``_aaiclick_type``
     * is written as is (the JSON form of a returned task DAG); any other
     * non-null value is wrapped as ``{"_aaiclick_type": "native", "value": …}``;
     * ``void`` / ``null`` writes nothing. */
    private static void executeTask(String entrypoint, Path io) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rawKwargs = mapper.readTree(Files.readString(io.resolve(IN_JSON)));
        if (!rawKwargs.isObject()) {
            throw new IllegalStateException(IN_JSON + " is not a JSON object");
        }
        ObjectNode kwargs = KwargsResolver.unwrapKwargs((ObjectNode) rawKwargs, mapper);

        Method method = TaskRegistry.resolve(entrypoint);
        Object[] bound = KwargsBinder.bind(method, kwargs, mapper);
        Object result = method.invoke(null, bound);

        if (result == null || method.getReturnType() == void.class) {
            return;
        }
        JsonNode tree = mapper.valueToTree(result);
        JsonNode out = tree.isObject() && tree.has(KwargsResolver.AAICLICK_TYPE) ? tree : KwargsResolver.nativeRef(tree, mapper);
        Files.writeString(io.resolve(OUT_JSON), mapper.writeValueAsString(out));
    }

    private static String format(Throwable t) {
        return t.getClass().getSimpleName() + ": " + t.getMessage();
    }
}
