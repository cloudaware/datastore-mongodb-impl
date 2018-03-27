package com.cloudaware.store.model;

import com.google.common.base.MoreObjects;

import java.io.Serializable;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Base class for all Google Cloud Datastore value types.
 * All values must be associated with a non-null content (except {@link NullValue}).
 * All values are immutable (including their content). To edit (a copy) use {@link #toBuilder()}.
 * Unsupported value (deprecated or unrecognized) would be represented by {@link }.
 *
 * @param <V> the type of the content for this value
 */
public abstract class Value<V> implements Serializable {

    private static final long serialVersionUID = 8532411152601335280L;

    private final ValueType valueType;
    private final boolean excludeFromIndexes;
    private final V value;

    @SuppressWarnings("deprecation")
    <P extends Value<V>, B extends BaseBuilder<V, P, B>> Value(final ValueBuilder<V, P, B> builder) {
        valueType = builder.getValueType();
        excludeFromIndexes = builder.getExcludeFromIndexes();
        value = builder.get();
    }

    /**
     * Returns the type of this value.
     */
    public final ValueType getType() {
        return valueType;
    }

    /**
     * Returns whether this value should be excluded from indexes.
     */
    public final boolean excludeFromIndexes() {
        return excludeFromIndexes;
    }

    public final V get() {
        return value;
    }

    public abstract ValueBuilder<?, ?, ?> toBuilder();

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("valueType", valueType)
                .add("excludeFromIndexes", excludeFromIndexes)
                .add("value", value)
                .toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(valueType, excludeFromIndexes, value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (!getClass().isInstance(obj)) {
            return false;
        }
        final Value<V> other = (Value<V>) obj;
        return Objects.equals(valueType, other.valueType)
                && Objects.equals(excludeFromIndexes, other.excludeFromIndexes)
                && Objects.equals(value, other.value);
    }

    interface BuilderFactory<V, P extends Value<V>, B extends ValueBuilder<V, P, B>>
            extends java.io.Serializable {
        B newBuilder(V value);
    }

    abstract static class BaseBuilder<V, P extends Value<V>, B extends BaseBuilder<V, P, B>>
            implements ValueBuilder<V, P, B> {

        private final ValueType valueType;
        private boolean excludeFromIndexes;
        private V value;

        BaseBuilder(final ValueType valueType) {
            this.valueType = valueType;
        }

        @Override
        public ValueType getValueType() {
            return valueType;
        }

        @Override
        public B mergeFrom(final P other) {
            excludeFromIndexes = other.excludeFromIndexes();
            set(other.get());
            return self();
        }

        @Override
        public boolean getExcludeFromIndexes() {
            return excludeFromIndexes;
        }

        @Override
        public B setExcludeFromIndexes(final boolean excludeFromIndexesArg) {
            this.excludeFromIndexes = excludeFromIndexesArg;
            return self();
        }

        @Override
        public V get() {
            return value;
        }

        @Override
        public B set(final V valueArg) {
            this.value = checkNotNull(valueArg);
            return self();
        }

        @SuppressWarnings("unchecked")
        private B self() {
            return (B) this;
        }

        @Override
        public abstract P build();
    }

//  @SuppressWarnings("unchecked")
//  com.google.datastore.v1.Value toPb() {
//    return getType().getMarshaller().toProto(this);
//  }
//
//  static Value<?> fromPb(com.google.datastore.v1.Value proto) {
//    ValueTypeCase descriptorId = proto.getValueTypeCase();
//    ValueType valueType = ValueType.getByDescriptorId(descriptorId.getNumber());
//    return valueType == null ? RawValue.MARSHALLER.fromProto(proto).build()
//            : valueType.getMarshaller().fromProto(proto).build();
//  }
}
