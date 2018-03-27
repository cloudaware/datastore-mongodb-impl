package com.cloudaware.store.model;

import com.google.common.base.Preconditions;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An entity is the Google Cloud Datastore persistent data object for a specific key.
 * An entity will always have a complete {@link Key}.
 */
public final class Entity extends FullEntity<Key> {

    private static final long serialVersionUID = 2312315289215899118L;

    Entity(final Builder builder) {
        super(builder);
    }

    Entity(final FullEntity<Key> from) {
        super(from);
        Preconditions.checkArgument(from.getKey() != null);
    }

    static Entity convert(final FullEntity<Key> from) {
        if (from instanceof Entity) {
            return (Entity) from;
        }
        return new Entity(from);
    }

    public static Builder newBuilder(final Key key) {
        return new Builder(key);
    }

    public static Builder newBuilder(final Entity copyFrom) {
        return new Builder(copyFrom);
    }

    public static Builder newBuilder(final Key key, final FullEntity<?> copyFrom) {
        return new Builder(key, copyFrom);
    }

    public static final class Builder extends BaseEntity.Builder<Key, Builder> {

        private Builder() {
        }

        private Builder(final Key key) {
            super(checkNotNull(key));
        }

        private Builder(final Entity entity) {
            super(entity);
        }

        private Builder(final Key key, final FullEntity<?> entity) {
            setProperties(entity.getProperties());
            setKey(key);
        }

        @Override
        public Builder setKey(final Key key) {
            super.setKey(checkNotNull(key));
            return this;
        }

        @Override
        public Entity build() {
            Preconditions.checkState(key() != null);
            return new Entity(this);
        }
    }

//  static Entity fromPb(com.google.datastore.v1.Entity entityPb) {
//    return new Builder().fill(entityPb).build();
//  }
}
