package io.github.kolodkin.aaiclick.task;

import java.sql.SQLException;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Mirror of the Python kwargs deserializer (``_deserialize_value``) for the
 * value shapes a jvm task supports: plain JSON values, ``native_value``
 * wrappers, and upstream task refs resolving to plain values. Object/View
 * refs, pydantic refs, callables, and group results fail with a clear error —
 * the JVM data plane is plain values only.
 */
public class KwargsResolver {

    static final String REF_TYPE = "ref_type";
    static final String UPSTREAM = "upstream";
    static final String TASK_ID = "task_id";
    static final String NATIVE_VALUE = "native_value";
    static final String OBJECT_TYPE = "object_type";
    static final String PYDANTIC_TYPE = "pydantic_type";

    private final TaskStore store;
    private final ObjectMapper mapper;

    public KwargsResolver(TaskStore store, ObjectMapper mapper) {
        this.store = store;
        this.mapper = mapper;
    }

    public JsonNode resolve(JsonNode node) {
        if (node.isArray()) {
            ArrayNode out = mapper.createArrayNode();
            for (JsonNode item : node) {
                out.add(resolve(item));
            }
            return out;
        }
        if (!node.isObject()) {
            return node;
        }
        ObjectNode obj = (ObjectNode) node;
        if (obj.has(REF_TYPE)) {
            return resolveRef(obj);
        }
        if (obj.has(NATIVE_VALUE) && obj.size() == 1) {
            return obj.get(NATIVE_VALUE);
        }
        if (obj.has(OBJECT_TYPE)) {
            throw new IllegalStateException(
                "Object/View refs are not supported in jvm task kwargs (plain values only)");
        }
        if (obj.has(PYDANTIC_TYPE)) {
            throw new IllegalStateException(
                "Pydantic refs are not supported in jvm task kwargs (plain values only)");
        }
        ObjectNode out = mapper.createObjectNode();
        for (Map.Entry<String, JsonNode> field : obj.properties()) {
            out.set(field.getKey(), resolve(field.getValue()));
        }
        return out;
    }

    private JsonNode resolveRef(ObjectNode obj) {
        String refType = obj.path(REF_TYPE).asText();
        if (!UPSTREAM.equals(refType)) {
            throw new IllegalStateException(
                "jvm tasks support only upstream refs in kwargs, got ref_type=" + refType);
        }
        long upstreamId = obj.path(TASK_ID).asLong();
        String resultJson;
        try {
            resultJson = store.loadCompletedResult(upstreamId);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to load upstream task " + upstreamId + " result", e);
        }
        if (resultJson == null) {
            return NullNode.getInstance();
        }
        try {
            return resolve(mapper.readTree(resultJson));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Upstream task " + upstreamId + " result is not valid JSON", e);
        }
    }
}
