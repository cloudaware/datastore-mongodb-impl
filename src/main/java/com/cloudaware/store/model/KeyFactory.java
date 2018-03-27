package com.cloudaware.store.model;

import com.google.common.collect.ImmutableList;

public final class KeyFactory extends BaseKey.Builder<KeyFactory> {

    private final String pi;
    private final String ns;

    public KeyFactory(final String projectId) {
        this(projectId, "");
    }

    public KeyFactory(final String projectId, final String namespace) {
        super(projectId);
        setNamespace(namespace);
        this.pi = projectId;
        this.ns = namespace;
    }

    public IncompleteKey newKey() {
        final ImmutableList<PathElement> path = ImmutableList.<PathElement>builder()
                .addAll(ancestors).add(PathElement.of(kind)).build();
        return new IncompleteKey(projectId, namespace, path);
    }

    public Key newKey(final String name) {
        final ImmutableList<PathElement> path = ImmutableList.<PathElement>builder()
                .addAll(ancestors).add(PathElement.of(kind, name)).build();
        return new Key(projectId, namespace, path);
    }

    public Key newKey(final long id) {
        final ImmutableList<PathElement> path = ImmutableList.<PathElement>builder()
                .addAll(ancestors).add(PathElement.of(kind, id)).build();
        return new Key(projectId, namespace, path);
    }

    /**
     * Resets the KeyFactory to its initial state.
     *
     * @return {@code this} for chaining
     */
    public KeyFactory reset() {
        setProjectId(pi);
        setNamespace(ns);
        kind = null;
        ancestors.clear();
        return this;
    }

    @Override
    protected IncompleteKey build() {
        return newKey();
    }
}

