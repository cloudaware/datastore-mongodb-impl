package com.cloudaware.store.model;

/**
 * A full entity is a {@link BaseEntity} that holds all the properties associated with a
 * Datastore entity (as opposed to {@link ProjectionEntity}).
 */
public class FullEntity<K extends IncompleteKey> extends BaseEntity<K> {

    private static final long serialVersionUID = -2075539363782670624L;

    FullEntity(final BaseEntity.Builder<K, ?> builder) {
        super(builder);
    }

    FullEntity(final FullEntity<K> from) {
        super(from);
    }

    public static Builder<IncompleteKey> newBuilder() {
        return new Builder<>();
    }

    public static <K extends IncompleteKey> Builder<K> newBuilder(final K key) {
        return new Builder<>(key);
    }

    public static <K extends IncompleteKey> Builder<K> newBuilder(final FullEntity<K> copyFrom) {
        return new Builder<>(copyFrom);
    }

    public static class Builder<K extends IncompleteKey> extends BaseEntity.Builder<K, Builder<K>> {

        Builder() {
        }

        Builder(final K key) {
            super(key);
        }

        Builder(final FullEntity<K> entity) {
            super(entity);
        }

        @Override
        public FullEntity<K> build() {
            return new FullEntity<>(this);
        }
    }

//    static FullEntity<?> fromPb(com.google.datastore.v1.Entity entityPb) {
//        return new Builder<>().fill(entityPb).build();
//    }
}
