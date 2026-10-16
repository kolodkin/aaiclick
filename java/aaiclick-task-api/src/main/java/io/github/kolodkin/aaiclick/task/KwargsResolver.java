package io.github.kolodkin.aaiclick.task;

import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Unwraps the tagged dicts ``in.json`` may carry. The host has already
 * resolved upstream refs into plain values, so the only tag left to honour is
 * ``native``; any other ``_aaiclick_type`` is a ref the JVM data plane cannot
 * carry (an Object, a callable) and fails with a clear error.
 */
public final class KwargsResolver {

    static final String AAICLICK_TYPE = "_aaiclick_type";
    static final String NATIVE = "native";
    static final String VALUE = "value";

    private KwargsResolver() {
    }

    /** Unwraps each top-level kwarg independently — the map itself is never a
     * ref, so a parameter named ``_aaiclick_type`` stays a parameter. */
    public static ObjectNode unwrapKwargs(ObjectNode kwargs, ObjectMapper mapper) {
        ObjectNode out = mapper.createObjectNode();
        for (Map.Entry<String, JsonNode> field : kwargs.properties()) {
            out.set(field.getKey(), unwrap(field.getValue(), mapper));
        }
        return out;
    }

    static JsonNode nativeRef(JsonNode value, ObjectMapper mapper) {
        ObjectNode ref = mapper.createObjectNode();
        ref.put(AAICLICK_TYPE, NATIVE);
        ref.set(VALUE, value);
        return ref;
    }

    private static JsonNode unwrap(JsonNode node, ObjectMapper mapper) {
        if (node.isArray()) {
            ArrayNode out = mapper.createArrayNode();
            for (JsonNode item : node) {
                out.add(unwrap(item, mapper));
            }
            return out;
        }
        if (!node.isObject()) {
            return node;
        }
        ObjectNode obj = (ObjectNode) node;
        if (!obj.has(AAICLICK_TYPE)) {
            return unwrapKwargs(obj, mapper);
        }
        String kind = obj.path(AAICLICK_TYPE).asText();
        if (NATIVE.equals(kind)) {
            return obj.get(VALUE);
        }
        throw new IllegalStateException(
            "jvm task kwargs carry plain values only, got a " + kind + " ref (" + AAICLICK_TYPE + ")");
    }
}
