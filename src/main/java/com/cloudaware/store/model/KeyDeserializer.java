package com.cloudaware.store.model;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;

import java.lang.reflect.Type;
import java.util.List;

public class KeyDeserializer implements JsonDeserializer<Key> {
    @Override
    public Key deserialize(final JsonElement json, final Type typeOfT, final JsonDeserializationContext context) throws JsonParseException {

        final ImmutableList.Builder<PathElement> builder = ImmutableList.<PathElement>builder();

        final JsonArray path = json.getAsJsonObject().get("path").getAsJsonArray();
        for (int i = 0; i < path.size(); i++) {
            final String pathKind = path.get(i).getAsJsonObject().get("kind") == null ? null : path.get(i).getAsJsonObject().get("kind").getAsString();
            final String pathName = path.get(i).getAsJsonObject().get("name") == null ? null : path.get(i).getAsJsonObject().get("name").getAsString();
            final Long pathId = path.get(i).getAsJsonObject().get("id") == null ? null : path.get(i).getAsJsonObject().get("id").getAsLong();
            if (pathName != null) {
                builder.add(PathElement.of(pathKind, pathName));
            } else if (pathId != null) {
                builder.add(PathElement.of(pathKind, pathId));
            } else {
                builder.add(PathElement.of(pathKind));
            }
        }

        final List<PathElement> pathElements = builder.build();

        final String projectId = json.getAsJsonObject().getAsJsonPrimitive("projectId") == null ? null : json.getAsJsonObject().getAsJsonPrimitive("projectId").getAsString();
        final String namespace = json.getAsJsonObject().getAsJsonPrimitive("namespace") == null ? null : json.getAsJsonObject().getAsJsonPrimitive("namespace").getAsString();

        final PathElement leaf = pathElements.get(pathElements.size() - 1);

        final Key.Builder keyBuilder;
        if (leaf.hasName()) {
            keyBuilder = Key.newBuilder(projectId, leaf.getKind(), leaf.getName()).setNamespace(namespace);
        } else if (leaf.hasId()) {
            keyBuilder = Key.newBuilder(projectId, leaf.getKind(), leaf.getId()).setNamespace(namespace);
        } else {
            throw new IllegalStateException("Cannot find name or id in serialized key");
        }
        for (int i = 0; i < pathElements.size() - 1; i++) {
            keyBuilder.addAncestor(pathElements.get(i));
        }

        return keyBuilder.build();
    }
}
