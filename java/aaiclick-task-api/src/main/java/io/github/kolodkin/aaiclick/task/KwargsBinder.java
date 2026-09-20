package io.github.kolodkin.aaiclick.task;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Binds resolved JSON kwargs to a task method's parameters by name, each
 * value converted to the parameter's (generic) type via Jackson. Missing and
 * unexpected keys are errors, mirroring Python's ``TypeError`` on bad
 * kwargs. */
public final class KwargsBinder {

    private KwargsBinder() {
    }

    public static Object[] bind(Method method, ObjectNode kwargs, ObjectMapper mapper) {
        Parameter[] params = method.getParameters();
        Set<String> names = new HashSet<>();
        for (Parameter param : params) {
            if (!param.isNamePresent()) {
                throw new IllegalStateException(
                    "Parameter names of " + method + " were compiled away — compile task classes "
                        + "with -parameters so kwargs can bind by name");
            }
            names.add(param.getName());
        }

        List<String> extra = new ArrayList<>();
        for (Map.Entry<String, JsonNode> field : kwargs.properties()) {
            if (!names.contains(field.getKey())) {
                extra.add(field.getKey());
            }
        }
        if (!extra.isEmpty()) {
            throw new IllegalArgumentException(
                "Unexpected kwargs " + extra + " for " + method + " (parameters: " + names + ")");
        }

        Object[] args = new Object[params.length];
        for (int i = 0; i < params.length; i++) {
            JsonNode value = kwargs.get(params[i].getName());
            if (value == null) {
                throw new IllegalArgumentException(
                    "Missing required kwarg '" + params[i].getName() + "' for " + method);
            }
            args[i] = mapper.convertValue(value, mapper.constructType(params[i].getParameterizedType()));
        }
        return args;
    }
}
