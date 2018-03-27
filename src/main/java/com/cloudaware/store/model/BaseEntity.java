package com.cloudaware.store.model;

import com.cloudaware.store.StoreException;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSortedMap;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.cloudaware.store.model.BlobValue.of;
import static com.cloudaware.store.model.BooleanValue.of;
import static com.cloudaware.store.model.DoubleValue.of;
import static com.cloudaware.store.model.EntityValue.of;
import static com.cloudaware.store.model.KeyValue.of;
import static com.cloudaware.store.model.ListValue.of;
import static com.cloudaware.store.model.LongValue.of;
import static com.cloudaware.store.model.NullValue.of;
import static com.cloudaware.store.model.StringValue.of;
import static com.cloudaware.store.model.TimestampValue.of;

/**
 * A base class for entities (key and properties).
 * An entity is a Google Cloud Datastore persistent data object.
 * An entity holds one or more properties, represented by a name (as {@link String})
 * and a value (as {@link com.cloudaware.store.model.Value}), and may be associated with a
 * key. For a list of possible values see {@link ValueType}.
 *
 * @see <a href="https://cloud.google.com/datastore/docs/concepts/entities">Google Cloud Datastore
 * Entities, Properties, and Keys</a>
 */
public abstract class BaseEntity<K extends IncompleteKey> implements Serializable {

    private static final long serialVersionUID = -9070588108769487081L;
    private final ImmutableSortedMap<String, Value<?>> properties;
    private final K key;

    BaseEntity(final Builder<K, ?> builder) {
        this.key = builder.key;
        this.properties = ImmutableSortedMap.copyOf(builder.properties);
    }

    BaseEntity(final BaseEntity<K> from) {
        this.key = from.getKey();
        this.properties = from.properties;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("key", key)
                .add("properties", properties)
                .toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, properties);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof BaseEntity)) {
            return false;
        }
        final BaseEntity<?> other = (BaseEntity<?>) obj;
        return Objects.equals(key, other.key)
                && Objects.equals(properties, other.properties);
    }

    /**
     * Returns true if entity has a non-null key.
     */
    public boolean hasKey() {
        return key != null;
    }

    /**
     * Returns the associated key or null if it does not have one.
     */
    public K getKey() {
        return key;
    }

    /**
     * Returns {@code true} if the entity contains a property with the given {@code name}.
     */
    public boolean contains(final String name) {
        return properties.containsKey(name);
    }

    /**
     * Returns the {@link Value} for the given property {@code name}.
     *
     * @throws StoreException if no such property
     */
    @SuppressWarnings("unchecked")
    public <V extends Value<?>> V getValue(final String name) {
        final V property = (V) properties.get(name);
        if (property == null) {
            throw StoreException.throwInvalidRequest("No such property %s", name);
        }
        return property;
    }

    /**
     * Returns true if property is an instance of NullValue.
     *
     * @throws StoreException if no such property
     */
    public boolean isNull(final String name) {
        return getValue(name) instanceof NullValue;
    }

    /**
     * Returns the property value as a string.
     *
     * @throws StoreException     if no such property
     * @throws ClassCastException if value is not a string
     */
    @SuppressWarnings("unchecked")
    public String getString(final String name) {
        return ((Value<String>) getValue(name)).get();
    }

    /**
     * Returns the property value as long.
     *
     * @throws StoreException     if no such property
     * @throws ClassCastException if value is not a long
     */
    @SuppressWarnings("unchecked")
    public long getLong(final String name) {
        return ((Value<Long>) getValue(name)).get();
    }

    /**
     * Returns the property value as a double.
     *
     * @throws StoreException     if no such property
     * @throws ClassCastException if value is not a double
     */
    @SuppressWarnings("unchecked")
    public double getDouble(final String name) {
        return ((Value<Double>) getValue(name)).get();
    }

    /**
     * Returns the property value as a boolean.
     *
     * @throws StoreException     if no such property
     * @throws ClassCastException if value is not a boolean
     */
    @SuppressWarnings("unchecked")
    public boolean getBoolean(final String name) {
        return ((Value<Boolean>) getValue(name)).get();
    }

    /**
     * Returns the property value as a Timestamp.
     *
     * @throws StoreException     if no such property
     * @throws ClassCastException if value is not a Timestamp
     */
    @SuppressWarnings("unchecked")
    public Timestamp getTimestamp(final String name) {
        return ((Value<Timestamp>) getValue(name)).get();
    }

    /**
     * Returns the property value as a Key.
     *
     * @throws StoreException     if no such property
     * @throws ClassCastException if value is not a Key
     */
    @SuppressWarnings("unchecked")
    public Key getKey(final String name) {
        return ((Value<Key>) getValue(name)).get();
    }

//    /**
//     * Returns the property value as a LatLng.
//     *
//     * @throws StoreException if no such property.
//     * @throws ClassCastException if value is not a LatLng.
//     */
//    @SuppressWarnings("unchecked")
//    public LatLng getLatLng(String name) {
//        return ((Value<LatLng>) getValue(name)).get();
//    }

    /**
     * Returns the property value as an entity.
     *
     * @throws StoreException     if no such property
     * @throws ClassCastException if value is not an entity
     */
    @SuppressWarnings("unchecked")
    public <K extends IncompleteKey> FullEntity<K> getEntity(final String name) {
        return ((Value<FullEntity<K>>) getValue(name)).get();
    }

    /**
     * Returns the property value as a list of values.
     *
     * @throws StoreException     if no such property
     * @throws ClassCastException if value is not a list of values
     */
    @SuppressWarnings("unchecked")
    public <T extends Value<?>> List<T> getList(final String name) {
        return (List<T>) getValue(name).get();
    }

    /**
     * Returns the property value as a blob.
     *
     * @throws StoreException     if no such property
     * @throws ClassCastException if value is not a blob
     */
    @SuppressWarnings("unchecked")
    public Binary getBlob(final String name) {
        return ((Value<Binary>) getValue(name)).get();
    }

    /**
     * Returns the properties name.
     */
    public Set<String> getNames() {
        return properties.keySet();
    }

    /**
     * return propeties
     *
     * @return properties
     */
    final ImmutableSortedMap<String, Value<?>> getProperties() {
        return properties;
    }

    public abstract static class Builder<K extends IncompleteKey, B extends Builder<K, B>> {

        private final Map<String, Value<?>> properties = new HashMap<>();
        private K key;

        Builder() {
        }

        Builder(final K key) {
            setKey(key);
        }

        Builder(final BaseEntity<K> entity) {
            this(entity.key, entity);
        }

        Builder(final K key, final BaseEntity<?> entity) {
            setKey(key);
            setProperties(entity.properties);
        }

        /**
         * return key
         *
         * @return
         */
        protected K key() {
            return key;
        }

        /**
         * return properties for modification
         *
         * @return
         */
        protected Map<String, Value<?>> getProperties() {
            return properties;
        }

        @SuppressWarnings("unchecked")
        private B self() {
            return (B) this;
        }

//        @SuppressWarnings("unchecked")
//        B fill(com.google.datastore.v1.Entity entityPb) {
//            Map<String, Value<?>> copiedProperties = Maps.newHashMap();
//            for (Map.Entry<String, com.google.datastore.v1.Value> entry :
//                    entityPb.getPropertiesMap().entrySet()) {
//                copiedProperties.put(entry.getKey(), Value.fromPb(entry.getValue()));
//            }
//            setProperties(copiedProperties);
//            if (entityPb.hasKey()) {
//                setKey((K) IncompleteKey.fromPb(entityPb.getKey()));
//            }
//            return self();
//        }

        /**
         * @param propertiesMap
         * @return
         */
        protected B setProperties(final Map<String, Value<?>> propertiesMap) {
            this.properties.putAll(propertiesMap);
            return self();
        }

        /**
         * Sets the key for the entity.
         */
        public B setKey(final K keyArg) {
            this.key = keyArg;
            return self();
        }

        /**
         * Clears all the properties.
         */
        public B clear() {
            properties.clear();
            return self();
        }

        /**
         * Removes a property with the given {@code name}.
         */
        public B remove(final String name) {
            properties.remove(name);
            return self();
        }

        /**
         * Sets a property.
         *
         * @param name  name of the property
         * @param value value associated with the property
         */
        public B set(final String name, final Value<?> value) {
            properties.put(name, value);
            return self();
        }

        /**
         * Sets a property of type {@link StringValue}.
         *
         * @param name  name of the property
         * @param value value associated with the property
         */
        public B set(final String name, final String value) {
            properties.put(name, of(value));
            return self();
        }

        /**
         * Sets a list property containing elements of type {@link StringValue}.
         *
         * @param name   name of the property
         * @param first  the first string in the list
         * @param second the second string in the list
         * @param others other strings in the list
         */
        public B set(final String name, final String first, final String second, final String... others) {
            final List<StringValue> values = new LinkedList<>();
            values.add(of(first));
            values.add(of(second));
            for (final String other : others) {
                values.add(of(other));
            }
            properties.put(name, of(values));
            return self();
        }

        /**
         * Sets a property of type {@link LongValue}.
         *
         * @param name  name of the property
         * @param value value associated with the property
         */
        public B set(final String name, final long value) {
            properties.put(name, of(value));
            return self();
        }

        /**
         * Sets a list property containing elements of type {@link LongValue}.
         *
         * @param name   name of the property
         * @param first  the first long in the list
         * @param second the second long in the list
         * @param others other longs in the list
         */
        public B set(final String name, final long first, final long second, final long... others) {
            final List<LongValue> values = new LinkedList<>();
            values.add(of(first));
            values.add(of(second));
            for (final long other : others) {
                values.add(of(other));
            }
            properties.put(name, of(values));
            return self();
        }

        /**
         * Sets a property of type {@link DoubleValue}.
         *
         * @param name  name of the property
         * @param value value associated with the property
         */
        public B set(final String name, final double value) {
            properties.put(name, of(value));
            return self();
        }

        /**
         * Sets a list property containing elements of type {@link DoubleValue}.
         *
         * @param name   name of the property
         * @param first  the first double in the list
         * @param second the second double in the list
         * @param others other doubles in the list
         */
        public B set(final String name, final double first, final double second, final double... others) {
            final List<DoubleValue> values = new LinkedList<>();
            values.add(of(first));
            values.add(of(second));
            for (final double other : others) {
                values.add(of(other));
            }
            properties.put(name, of(values));
            return self();
        }

        /**
         * Sets a property of type {@link BooleanValue}.
         *
         * @param name  name of the property
         * @param value value associated with the property
         */
        public B set(final String name, final boolean value) {
            properties.put(name, of(value));
            return self();
        }

        /**
         * Sets a list property containing elements of type {@link BooleanValue}.
         *
         * @param name   name of the property
         * @param first  the first boolean in the list
         * @param second the second boolean in the list
         * @param others other booleans in the list
         */
        public B set(final String name, final boolean first, final boolean second, final boolean... others) {
            final List<BooleanValue> values = new LinkedList<>();
            values.add(of(first));
            values.add(of(second));
            for (final boolean other : others) {
                values.add(of(other));
            }
            properties.put(name, of(values));
            return self();
        }

        /**
         * Sets a property of type {@link TimestampValue}.
         *
         * @param name  name of the property
         * @param value value associated with the property
         */
        public B set(final String name, final Timestamp value) {
            properties.put(name, of(value));
            return self();
        }

        /**
         * Sets a list property containing elements of type {@link TimestampValue}.
         *
         * @param name   name of the property
         * @param first  the first {@link Timestamp} in the list
         * @param second the second {@link Timestamp} in the list
         * @param others other {@link Timestamp}s in the list
         */
        public B set(final String name, final Timestamp first, final Timestamp second, final Timestamp... others) {
            final List<TimestampValue> values = new LinkedList<>();
            values.add(of(first));
            values.add(of(second));
            for (final Timestamp other : others) {
                values.add(of(other));
            }
            properties.put(name, of(values));
            return self();
        }

//        /**
//         * Sets a property of type {@link LatLng}.
//         *
//         * @param name name of the property
//         * @param value value associated with the property
//         */
//        public B set(String name, LatLng value) {
//            properties.put(name, of(value));
//            return self();
//        }

//        /**
//         * Sets a list property containing elements of type {@link LatLng}.
//         *
//         * @param name name of the property
//         * @param first the first {@link LatLng} in the list
//         * @param second the second {@link LatLng} in the list
//         * @param others other {@link LatLng}s in the list
//         */
//        public B set(String name, LatLng first, LatLng second, LatLng... others) {
//            List<LatLngValue> values = new LinkedList<>();
//            values.add(of(first));
//            values.add(of(second));
//            for (LatLng other : others) {
//                values.add(of(other));
//            }
//            properties.put(name, of(values));
//            return self();
//        }

        /**
         * Sets a property of type {@link KeyValue}.
         *
         * @param name  name of the property
         * @param value value associated with the property
         */
        public B set(final String name, final Key value) {
            properties.put(name, of(value));
            return self();
        }

        /**
         * Sets a list property containing elements of type {@link KeyValue}.
         *
         * @param name   name of the property
         * @param first  the first {@link Key} in the list
         * @param second the second {@link Key} in the list
         * @param others other {@link Key}s in the list
         */
        public B set(final String name, final Key first, final Key second, final Key... others) {
            final List<KeyValue> values = new LinkedList<>();
            values.add(of(first));
            values.add(of(second));
            for (final Key other : others) {
                values.add(of(other));
            }
            properties.put(name, of(values));
            return self();
        }

        /**
         * Sets a property of type {@link EntityValue}.
         *
         * @param name  name of the property
         * @param value value associated with the property
         */
        public B set(final String name, final FullEntity<?> value) {
            properties.put(name, of(value));
            return self();
        }

        /**
         * Sets a list property containing elements of type {@link EntityValue}.
         *
         * @param name   name of the property
         * @param first  the first {@link FullEntity} in the list
         * @param second the second {@link FullEntity} in the list
         * @param others other entities in the list
         */
        public B set(final String name, final FullEntity<?> first, final FullEntity<?> second, final FullEntity<?>... others) {
            final List<EntityValue> values = new LinkedList<>();
            values.add(of(first));
            values.add(of(second));
            for (final FullEntity<?> other : others) {
                values.add(of(other));
            }
            properties.put(name, of(values));
            return self();
        }

        /**
         * Sets a property of type {@link ListValue}.
         *
         * @param name   name of the property
         * @param values list of values associated with the property
         */
        public B set(final String name, final List<? extends Value<?>> values) {
            properties.put(name, of(values));
            return self();
        }

        /**
         * Sets a property of type {@link ListValue}.
         *
         * @param name   name of the property
         * @param first  the first value in the list
         * @param second the second value in the list
         * @param others other values in the list
         */
        public B set(final String name, final Value<?> first, final Value<?> second, final Value<?>... others) {
            properties.put(name, ListValue.newBuilder().addValue(first).addValue(second, others).build());
            return self();
        }

        /**
         * Sets a property of type {@link BlobValue}.
         *
         * @param name  name of the property
         * @param value value associated with the property
         */
        public B set(final String name, final Binary value) {
            properties.put(name, of(value));
            return self();
        }

        /**
         * Sets a list property containing elements of type {@link BlobValue}.
         *
         * @param name   name of the property
         * @param first  the first {@link Binary} in the list
         * @param second the second {@link Binary} in the list
         * @param others other {@link Binary}s in the list
         */
        public B set(final String name, final Binary first, final Binary second, final Binary... others) {
            final List<BlobValue> values = new LinkedList<>();
            values.add(of(first));
            values.add(of(second));
            for (final Binary other : others) {
                values.add(of(other));
            }
            properties.put(name, of(values));
            return self();
        }

        /**
         * Sets a property of type {@code NullValue}.
         *
         * @param name name of the property
         */
        public B setNull(final String name) {
            properties.put(name, of());
            return self();
        }

        public abstract BaseEntity<K> build();
    }

//todo: sertialiozation
//    final com.google.datastore.v1.Entity toPb() {
//        com.google.datastore.v1.Entity.Builder entityPb = com.google.datastore.v1.Entity.newBuilder();
//        for (Map.Entry<String, Value<?>> entry : properties.entrySet()) {
//            entityPb.putProperties(entry.getKey(), entry.getValue().toPb());
//        }
//        if (key != null) {
//            entityPb.setKey(key.toPb());
//        }
//        return entityPb.build();
//    }
}
