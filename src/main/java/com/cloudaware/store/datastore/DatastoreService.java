package com.cloudaware.store.datastore;

import com.cloudaware.store.Marshaller;
import com.cloudaware.store.StoreException;
import com.cloudaware.store.StoreService;
import com.cloudaware.store.Unmarshaller;
import com.cloudaware.store.model.Binary;
import com.cloudaware.store.model.BlobValue;
import com.cloudaware.store.model.BooleanValue;
import com.cloudaware.store.model.DoubleValue;
import com.cloudaware.store.model.Entity;
import com.cloudaware.store.model.EntityValue;
import com.cloudaware.store.model.FullEntity;
import com.cloudaware.store.model.IncompleteKey;
import com.cloudaware.store.model.Key;
import com.cloudaware.store.model.KeyFactory;
import com.cloudaware.store.model.KeyValue;
import com.cloudaware.store.model.ListValue;
import com.cloudaware.store.model.LongValue;
import com.cloudaware.store.model.NullValue;
import com.cloudaware.store.model.PathElement;
import com.cloudaware.store.model.ProjectionEntity;
import com.cloudaware.store.model.Query;
import com.cloudaware.store.model.QueryResults;
import com.cloudaware.store.model.StringValue;
import com.cloudaware.store.model.Timestamp;
import com.cloudaware.store.model.TimestampValue;
import com.cloudaware.store.model.Value;
import com.google.cloud.datastore.Blob;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public final class DatastoreService implements StoreService {

    public static final DatastoreMarshaller MARSHALLER = new DatastoreMarshaller();
    public static final DatastoreUnmarshaller UNMARSHALLER = new DatastoreUnmarshaller();
    private final Datastore datastore;

    public DatastoreService() {
        this.datastore = DatastoreOptions.getDefaultInstance().getService();
    }

    public DatastoreService(final Datastore datastore) {
        this.datastore = datastore;
    }

    public Datastore getDatastore() {
        return datastore;
    }

    @Override
    public KeyFactory newKeyFactory() {
        return new KeyFactory(this.datastore.getOptions().getProjectId(), this.datastore.getOptions().getNamespace());
    }

    @Override
    public List<Entity> put(final Iterable<Entity> entities, final String... args) {

        final List<com.google.cloud.datastore.Entity> res = this.datastore.put(
                StreamSupport.stream(entities.spliterator(), false)
                        .map(e -> MARSHALLER.convertEntity(e))
                        .collect(Collectors.toList())
                        .toArray(new com.google.cloud.datastore.Entity[0])
        );
        return res.stream().map(e -> UNMARSHALLER.convertEntity(e)).collect(Collectors.toList());
    }

    @Override
    public Entity put(final Entity entity, final String... args) {
        return UNMARSHALLER.convertEntity(this.datastore.put(MARSHALLER.convertEntity(entity)));
    }

    @Override
    public Iterator<Entity> get(final Iterable<Key> keys, final String... args) {
        //todo: make iterable
        final List<Entity> out = Lists.newArrayList();
        for (final Key key : keys) {
            out.add(get(key));
        }
        return out.iterator();
    }

    @Override
    public Entity get(final Key key, final String... args) {
        final com.google.cloud.datastore.Entity entity = this.datastore.get(MARSHALLER.convertKey(key));
        return entity == null ? null : UNMARSHALLER.convertEntity(entity);
    }

    @Override
    public <T> QueryResults<T> run(final Query<T> query) {
        return new DatastoreQueryResultsImpl<>(this, query);
    }

    public void delete(final Iterable<Key> keys) {
        final Set<com.google.cloud.datastore.Key> res = Sets.newHashSet();
        for (final Key key : keys) {
            res.add(MARSHALLER.convertKey(key));
        }
        this.datastore.delete(res.toArray(new com.google.cloud.datastore.Key[0]));
    }

    public static final class DatastoreUnmarshaller implements Unmarshaller<
            com.google.cloud.datastore.Entity,
            com.google.cloud.datastore.FullEntity,
            com.google.cloud.datastore.ProjectionEntity,
            com.google.cloud.datastore.Key,
            com.google.cloud.datastore.IncompleteKey,
            com.google.cloud.datastore.Value> {
        public Key convertKey(final com.google.cloud.datastore.Key key) {
            final Key.Builder builder;
            if (key.hasId()) {
                builder = Key.newBuilder(key.getProjectId(), key.getKind(), key.getId());
            } else {
                builder = Key.newBuilder(key.getProjectId(), key.getKind(), key.getName());
            }
            builder.setNamespace(key.getNamespace());
            key.getAncestors().forEach(
                    a -> {
                        if (a.hasId()) {
                            builder.addAncestor(PathElement.of(a.getKind(), a.getId()));
                        } else {
                            builder.addAncestor(PathElement.of(a.getKind(), a.getName()));
                        }
                    }
            );
            return builder.build();
        }

        public IncompleteKey convertIncompleteKey(final com.google.cloud.datastore.IncompleteKey key) {
            final IncompleteKey.Builder builder = Key.newBuilder(key.getProjectId(), key.getKind());
            builder.setNamespace(key.getNamespace());
            key.getAncestors().forEach(
                    a -> {
                        if (a.hasId()) {
                            builder.addAncestor(PathElement.of(a.getKind(), a.getId()));
                        } else {
                            builder.addAncestor(PathElement.of(a.getKind(), a.getName()));
                        }
                    }
            );
            return builder.build();
        }

        public Entity convertEntity(final com.google.cloud.datastore.Entity entity) {

            final Entity.Builder builder = Entity.newBuilder(convertKey(entity.getKey()));
            for (final String name : entity.getNames()) {

                builder.set(name, convertValue(entity.getValue(name)));
            }
            return builder.build();
        }

        public FullEntity convertFullEntity(final com.google.cloud.datastore.FullEntity entity) {
            final com.google.cloud.datastore.IncompleteKey key = entity.getKey();
            final FullEntity.Builder builder;
            if (key instanceof com.google.cloud.datastore.Key) {
                builder = FullEntity.newBuilder(convertKey((com.google.cloud.datastore.Key) key));
            } else {
                builder = FullEntity.newBuilder();
                builder.setKey(convertIncompleteKey(key));
            }
            for (final String name : (Set<String>) entity.getNames()) {
                builder.set(name, convertValue(entity.getValue(name)));
            }
            return builder.build();
        }

        @Override
        public ProjectionEntity convertProjectionEntity(final com.google.cloud.datastore.ProjectionEntity entity) {
            final ProjectionEntity.Builder builder = ProjectionEntity.newBuilder(convertKey(entity.getKey()));
            for (final String name : entity.getNames()) {
                builder.set(name, convertValue(entity.getValue(name)));
            }
            return builder.build();
        }

        public Value<?> convertValue(final com.google.cloud.datastore.Value value) {
            switch (value.getType()) {
                case KEY:
                    return KeyValue.newBuilder(convertKey(((com.google.cloud.datastore.KeyValue) value).get())).setExcludeFromIndexes(value.excludeFromIndexes()).build();
                case LONG:
                    return LongValue.newBuilder(((com.google.cloud.datastore.LongValue) value).get()).setExcludeFromIndexes(value.excludeFromIndexes()).build();
                case DOUBLE:
                    return DoubleValue.newBuilder(((com.google.cloud.datastore.DoubleValue) value).get()).setExcludeFromIndexes(value.excludeFromIndexes()).build();
                case BOOLEAN:
                    return BooleanValue.newBuilder(((com.google.cloud.datastore.BooleanValue) value).get()).setExcludeFromIndexes(value.excludeFromIndexes()).build();
                case BLOB:
                    return BlobValue.newBuilder(new Binary(((com.google.cloud.datastore.BlobValue) value).get().toByteArray())).setExcludeFromIndexes(value.excludeFromIndexes()).build();
                case ENTITY:
                    final com.google.cloud.datastore.FullEntity val = ((com.google.cloud.datastore.EntityValue) value).get();

                    return EntityValue.newBuilder(
                            val instanceof com.google.cloud.datastore.Entity
                                    ? convertEntity((com.google.cloud.datastore.Entity) val)
                                    : convertFullEntity(val)
                    ).setExcludeFromIndexes(value.excludeFromIndexes()).build();
                case LIST:
                    return ListValue.newBuilder().set(
                            ((com.google.cloud.datastore.ListValue) value)
                                    .get()
                                    .stream()
                                    .map(v -> convertValue(v))
                                    .collect(Collectors.toList())
                    ).setExcludeFromIndexes(value.excludeFromIndexes()).build();
                case NULL:
                    return NullValue.of();
                case STRING:
                    return StringValue.newBuilder(((com.google.cloud.datastore.StringValue) value).get()).setExcludeFromIndexes(value.excludeFromIndexes()).build();
                case TIMESTAMP:
                    final com.google.cloud.Timestamp timestamp = ((com.google.cloud.datastore.TimestampValue) value).get();
                    return TimestampValue.newBuilder(
                            Timestamp.ofTimeSecondsAndNanos(
                                    timestamp.getSeconds(),
                                    timestamp.getNanos()
                            )
                    ).setExcludeFromIndexes(value.excludeFromIndexes()).build();
                default:
                    throw new StoreException("Cannot convert Value: " + value.toString());
            }

        }
    }

    public static final class DatastoreMarshaller implements Marshaller<
            com.google.cloud.datastore.Entity,
            com.google.cloud.datastore.FullEntity,
            com.google.cloud.datastore.Key,
            com.google.cloud.datastore.IncompleteKey,
            com.google.cloud.datastore.Value> {
        public com.google.cloud.datastore.Key convertKey(final Key in) {
            com.google.cloud.datastore.Key.Builder kb = null;
            if (in.hasId()) {
                kb = com.google.cloud.datastore.Key.newBuilder(in.getProjectId(), in.getKind(), in.getId());
            } else if (in.hasName()) {
                kb = com.google.cloud.datastore.Key.newBuilder(in.getProjectId(), in.getKind(), in.getName());
            }
            if (kb != null) {
                kb.setNamespace(in.getNamespace());
                kb.addAncestors(convertAncestors(in.getAncestors()));
                return kb.build();
            }
            throw new StoreException("Cannot coonvert Key: " + in.toString());

        }

        public com.google.cloud.datastore.IncompleteKey convertIncompleteKey(final IncompleteKey in) {
            final com.google.cloud.datastore.IncompleteKey.Builder kb = com.google.cloud.datastore.IncompleteKey.newBuilder(in.getProjectId(), in.getKind());
            if (kb != null) {
                kb.setNamespace(in.getNamespace());
                kb.addAncestors(convertAncestors(in.getAncestors()));
                return kb.build();
            }
            throw new StoreException("Cannot convert IncompleteKey: " + in.toString());
        }

        public com.google.cloud.datastore.Value<?> convertValue(final Value value) {
            switch (value.getType()) {
                case KEY:
                    return com.google.cloud.datastore.KeyValue.newBuilder(convertKey(((KeyValue) value).get())).setExcludeFromIndexes(value.excludeFromIndexes()).build();
                case LONG:
                    return com.google.cloud.datastore.LongValue.newBuilder(((LongValue) value).get()).setExcludeFromIndexes(value.excludeFromIndexes()).build();
                case DOUBLE:
                    return com.google.cloud.datastore.DoubleValue.newBuilder(((DoubleValue) value).get()).setExcludeFromIndexes(value.excludeFromIndexes()).build();
                case BOOLEAN:
                    return com.google.cloud.datastore.BooleanValue.newBuilder(((BooleanValue) value).get()).setExcludeFromIndexes(value.excludeFromIndexes()).build();
                case BLOB:
                    return com.google.cloud.datastore.BlobValue.newBuilder(Blob.copyFrom(((BlobValue) value).get().getData())).setExcludeFromIndexes(value.excludeFromIndexes()).build();
                case ENTITY:
                    return com.google.cloud.datastore.EntityValue.newBuilder(convertFullEntity(((EntityValue) value).get())).setExcludeFromIndexes(value.excludeFromIndexes()).build();
                case LIST:
                    return com.google.cloud.datastore.ListValue.newBuilder().set(
                            ((ListValue) value)
                                    .get()
                                    .stream()
                                    .map(v -> convertValue(v))
                                    .collect(Collectors.toList())
                    ).setExcludeFromIndexes(value.excludeFromIndexes()).build();
                case NULL:
                    return com.google.cloud.datastore.NullValue.of();
                case STRING:
                    return com.google.cloud.datastore.StringValue.newBuilder(((StringValue) value).get()).setExcludeFromIndexes(value.excludeFromIndexes()).build();
                case TIMESTAMP:
                    final Timestamp timestamp = ((TimestampValue) value).get();
                    return com.google.cloud.datastore.TimestampValue.newBuilder(
                            com.google.cloud.Timestamp.ofTimeSecondsAndNanos(
                                    timestamp.getSeconds(),
                                    timestamp.getNanos()
                            )
                    ).setExcludeFromIndexes(value.excludeFromIndexes()).build();
                default:
                    throw new StoreException("Cannot convert Value: " + value.toString());
            }

        }

        public com.google.cloud.datastore.Entity convertEntity(final Entity entity) {
            final com.google.cloud.datastore.Entity.Builder builder = com.google.cloud.datastore.Entity.newBuilder(convertKey(entity.getKey()));
            for (final String name : entity.getNames()) {
                builder.set(name, convertValue(entity.getValue(name)));
            }
            return builder.build();
        }

        public com.google.cloud.datastore.FullEntity convertFullEntity(final FullEntity entity) {

            final IncompleteKey key = entity.getKey();
            final com.google.cloud.datastore.FullEntity.Builder builder = com.google.cloud.datastore.Entity.newBuilder(
                    key instanceof Key
                            ? convertKey((Key) key)
                            : convertIncompleteKey(key)
            );
            for (final String name : (Set<String>) entity.getNames()) {
                builder.set(name, convertValue(entity.getValue(name)));
            }
            return builder.build();
        }

        private List<com.google.cloud.datastore.PathElement> convertAncestors(final List<PathElement> ancestors) {
            return ancestors.stream().map(
                    i -> {
                        if (i.getId() != null) {
                            return com.google.cloud.datastore.PathElement.of(i.getKind(), i.getId());
                        } else {
                            return com.google.cloud.datastore.PathElement.of(i.getKind(), i.getName());
                        }
                    }
            ).collect(Collectors.toList());
        }

    }
}
