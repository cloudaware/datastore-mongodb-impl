package com.cloudaware.store.model;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * An incomplete key (without a name or id).
 * This class is immutable.
 */
public class IncompleteKey extends BaseKey {

    private static final long serialVersionUID = 4947014765344279019L;

    IncompleteKey(final String projectId, final String namespace, final ImmutableList<PathElement> path) {
        super(projectId, namespace, path);
    }

    public static Builder newBuilder(final String projectId, final String kind) {
        return new Builder(projectId, kind);
    }

//    static IncompleteKey fromPb(com.google.datastore.v1.Key keyPb) {
//        String projectId = "";
//        String namespace = "";
//        if (keyPb.hasPartitionId()) {
//            com.google.datastore.v1.PartitionId partitionIdPb = keyPb.getPartitionId();
//            projectId = partitionIdPb.getProjectId();
//            namespace = partitionIdPb.getNamespaceId();
//        }
//        List<com.google.datastore.v1.Key.PathElement> pathElementsPb = keyPb.getPathList();
//        Preconditions.checkArgument(!pathElementsPb.isEmpty(), "Path must not be empty");
//        ImmutableList.Builder<PathElement> pathBuilder = ImmutableList.builder();
//        for (com.google.datastore.v1.Key.PathElement pathElementPb : pathElementsPb) {
//            pathBuilder.add(PathElement.fromPb(pathElementPb));
//        }
//        ImmutableList<PathElement> path = pathBuilder.build();
//        PathElement leaf = path.get(path.size() - 1);
//        if (leaf.getNameOrId() != null) {
//            return new Key(projectId, namespace, path);
//        }
//        return new IncompleteKey(projectId, namespace, path);
//    }

    public static Builder newBuilder(final IncompleteKey copyFrom) {
        return new Builder(copyFrom);
    }

    public static Builder newBuilder(final Key parent, final String kind) {
        return newBuilder(parent.getProjectId(), kind)
                .setNamespace(parent.getNamespace())
                .addAncestors(parent.getPath());
    }

    /**
     * Returns the key's parent.
     */
    @Override
    public Key getParent() {
        final List<PathElement> ancestors = getAncestors();
        if (ancestors.isEmpty()) {
            return null;
        }
        final PathElement parent = ancestors.get(ancestors.size() - 1);
        final Key.Builder keyBuilder;
        if (parent.hasName()) {
            keyBuilder = Key.newBuilder(getProjectId(), parent.getKind(), parent.getName());
        } else {
            keyBuilder = Key.newBuilder(getProjectId(), parent.getKind(), parent.getId());
        }
        final String namespace = getNamespace();
        if (namespace != null) {
            keyBuilder.setNamespace(namespace);
        }
        return keyBuilder.addAncestors(ancestors.subList(0, ancestors.size() - 1)).build();
    }

    public static final class Builder extends BaseKey.Builder<Builder> {

        private Builder(final String projectId, final String kind) {
            super(projectId, kind);
        }

        private Builder(final IncompleteKey copyFrom) {
            super(copyFrom);
        }

        @Override
        public IncompleteKey build() {
            final ImmutableList<PathElement> path = ImmutableList.<PathElement>builder()
                    .addAll(ancestors).add(PathElement.of(kind)).build();
            return new IncompleteKey(projectId, namespace, path);
        }
    }
}
