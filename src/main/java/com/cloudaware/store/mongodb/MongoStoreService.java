package com.cloudaware.store.mongodb;

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
import com.cloudaware.store.model.ProjectionEntity;
import com.cloudaware.store.model.Query;
import com.cloudaware.store.model.QueryResults;
import com.cloudaware.store.model.StringValue;
import com.cloudaware.store.model.Timestamp;
import com.cloudaware.store.model.TimestampValue;
import com.cloudaware.store.model.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public final class MongoStoreService implements StoreService {
    public static final String KEY_FIELD = "__key__";
    public static final MongoMarshaller MARSHALLER = new MongoMarshaller();
    public static final MongoUnmarshaller UNMARSHALLER = new MongoUnmarshaller();
    private static final int MAX_CONNECTION_IDLE_TIME = 60000;
    private final MongoClient client;
    private final String defaultProjectId;

    public MongoStoreService(final MongoClient client, final String defaultProjectId) {
        this.client = client;
        this.defaultProjectId = defaultProjectId;
    }

    public MongoStoreService(final String mongoClientUri, final String defaultProjectId) {
        client = new MongoClient(new MongoClientURI(mongoClientUri, MongoClientOptions.builder().socketKeepAlive(true).maxConnectionIdleTime(MAX_CONNECTION_IDLE_TIME)));
        this.defaultProjectId = defaultProjectId;
    }

    protected MongoDatabase getDatabase(final String projectId) {
        return client.getDatabase(projectId);
    }

    protected MongoCollection<BsonDocument> getCollection(final String projectId, final String namespace, final String kind) {
        final MongoDatabase db = getDatabase(projectId);
        final String collectionName = (namespace == null ? "" : namespace) + "__" + kind;
        return db.getCollection(collectionName, BsonDocument.class);
    }

    protected MongoCollection<BsonDocument> getCollection(final Key key) {
        return getCollection(key.getProjectId(), key.getNamespace(), key.getKind());
    }

    @Override
    public KeyFactory newKeyFactory() {
        return new KeyFactory(this.defaultProjectId, "");
    }

    @Override
    public List<Entity> put(final Iterable<Entity> entities, final String... args) {
        final List<Entity> out = Lists.newArrayList();
        final UpdateOptions updateOptions = new UpdateOptions();
        updateOptions.upsert(true);
        for (final Entity entity : entities) {
            final BsonDocument upsert = MARSHALLER.convertEntity(entity);
            upsert.remove(KEY_FIELD);
            getCollection(entity.getKey()).updateOne(new BsonDocument(KEY_FIELD, MARSHALLER.convertKey(entity.getKey())), new BsonDocument("$set", upsert), updateOptions);
            out.add(get(entity.getKey()));
        }
        return out;
    }

    @Override
    public Entity put(final Entity entity, final String... args) {
        return put(ImmutableList.of(entity)).get(0);
    }

    @Override
    public Iterator<Entity> get(final Iterable<Key> keys, final String... args) {
        final List<Entity> out = Lists.newArrayList();
        for (final Key key : keys) {
            out.add(get(key));
        }
        return out.iterator();
    }

    @Override
    public Entity get(final Key key, final String... args) {
        final BsonDocument filter = new BsonDocument(KEY_FIELD, MARSHALLER.convertKey(key));
        final FindIterable<BsonDocument> iterable = getCollection(key).find(filter);
        final BsonDocument document = iterable.first();
        return document == null ? null : UNMARSHALLER.convertEntity(document);
    }

    @Override
    public void delete(final Iterable<Key> keys) {
        for (final Key key : keys) {
            getCollection(key).findOneAndDelete(new BsonDocument(KEY_FIELD, MARSHALLER.convertIncompleteKey(key)));
        }
    }

    @Override
    public <T> QueryResults<T> run(final Query<T> query) {
        return new MongoQueryResultsImpl<T>(this, defaultProjectId, query);
    }

    public static final class MongoUnmarshaller implements Unmarshaller<BsonDocument, BsonDocument, BsonDocument, BsonDocument, BsonDocument, BsonValue> {

        public Entity convertEntity(final BsonDocument document) {
            final Key key = convertKey(document.getDocument(KEY_FIELD));
            final Entity.Builder builder = Entity.newBuilder(key);
            for (final String name : document.keySet()) {
                if (KEY_FIELD.equals(name) || "_id".equals(name)) {
                    continue;
                }
                builder.set(name, convertValue(document.get(name)));
            }
            return builder.build();
        }

        @Override
        public FullEntity convertFullEntity(final BsonDocument entity) {
            throw new UnsupportedOperationException();
        }

        public Key convertKey(final BsonDocument keyDocument) {
            if (keyDocument.keySet().size() == 2 && keyDocument.keySet().contains("type") && keyDocument.keySet().contains("key")) {
                final String type = keyDocument.getString("type").getValue();
                if (type != null) {
                    final String safe = keyDocument.getString("key").getValue();
                    if ("key".equals(type)) {
                        return Key.fromUrlSafe(safe);
                    } else if ("incompleteKey".equals(type)) {
                        throw new UnsupportedOperationException();
                    }
                }
            }
            throw new StoreException("Cannot deserialize key from mongo" + keyDocument);
        }

        @Override
        public ProjectionEntity convertProjectionEntity(final BsonDocument document) {
            final Key key = convertKey(document.getDocument(KEY_FIELD));
            final ProjectionEntity.Builder builder = ProjectionEntity.newBuilder(key);
            for (final String name : document.keySet()) {
                if (KEY_FIELD.equals(name) || "_id".equals(name)) {
                    continue;
                }
                builder.set(name, convertValue(document.get(name)));
            }
            return builder.build();
        }

        @Override
        public IncompleteKey convertIncompleteKey(final BsonDocument key) {
            throw new UnsupportedOperationException();
        }

        public Value<?> convertValue(final BsonValue value) {
            switch (value.getBsonType()) {
                case BOOLEAN:
                    return new BooleanValue(((BsonBoolean) value).getValue());
                case DOUBLE:
                    return new DoubleValue(((BsonDouble) value).doubleValue());
                case NULL:
                    return new NullValue();
                case ARRAY:
                    final BsonArray bsonArray = (BsonArray) value;
                    final List<Value<?>> listValue = Lists.newArrayList();
                    for (final BsonValue arrValue : bsonArray.getValues()) {
                        listValue.add(convertValue(arrValue));
                    }
                    return new ListValue(listValue);
                case DATE_TIME:
                    final long millis = ((BsonDateTime) value).getValue();
                    return new TimestampValue(Timestamp.ofTimeMicroseconds(TimeUnit.MILLISECONDS.toMicros(millis)));
                case STRING:
                    return new StringValue(((BsonString) value).getValue());
                case INT64:
                    return new LongValue(((BsonInt64) value).getValue());
                case BINARY:
                    return new BlobValue(new Binary(((BsonBinary) value).getData()));
                case DOCUMENT:
                    final BsonDocument doc = value.asDocument();
                    if (doc.keySet().size() == 2 && doc.keySet().contains("key") && doc.keySet().contains("type")) {
                        return new KeyValue(convertKey(doc));
                    } else {
                        return new EntityValue(convertEntity(doc));
                    }
                default:
                    throw new StoreException("Cannot convert Value: " + value.toString());
            }
        }
    }

    public static final class MongoMarshaller implements Marshaller<BsonDocument, BsonDocument, BsonDocument, BsonDocument, BsonValue> {

        public BsonDocument convertEntity(final Entity entity) {
            final BsonDocument bsonDocument = new BsonDocument();
            bsonDocument.append(KEY_FIELD, convertIncompleteKey(entity.getKey()));
            for (final String name : entity.getNames()) {
                bsonDocument.append(name, convertValue(entity.getValue(name)));
            }
            return bsonDocument;
        }

        public BsonDocument convertFullEntity(final FullEntity entity) {
            final BsonDocument bsonDocument = new BsonDocument();
            bsonDocument.append(KEY_FIELD, convertIncompleteKey(entity.getKey()));
            for (final String name : (Set<String>) entity.getNames()) {
                bsonDocument.append(name, convertValue(entity.getValue(name)));
            }
            return bsonDocument;
        }

        public BsonDocument convertKey(final Key key) {
            final BsonDocument bsonDocument = new BsonDocument();
            final BsonValue type = new BsonString("key");
            bsonDocument.append("type", type);
            bsonDocument.append("key", new BsonString(key.toUrlSafe()));
            return bsonDocument;
        }

        public BsonDocument convertIncompleteKey(final IncompleteKey key) {
            if (key instanceof Key) {
                return convertKey((Key) key);
            } else {
                throw new UnsupportedOperationException();
            }

        }

        public BsonValue convertValue(final Value value) {
            switch (value.getType()) {
                case KEY:
                    return convertIncompleteKey(((KeyValue) value).get());
                case STRING:
                    return new BsonString(((StringValue) value).get());
                case TIMESTAMP:
                    long milliseconds = TimeUnit.SECONDS.toMillis(((TimestampValue) value).get().getSeconds());
                    milliseconds += TimeUnit.NANOSECONDS.toMillis(((TimestampValue) value).get().getNanos());
                    return new BsonDateTime(milliseconds);
                case NULL:
                    return new BsonNull();
                case LIST:
                    final BsonArray bsonArray = new BsonArray();
                    for (final Value arr : ((ListValue) value).get()) {
                        bsonArray.add(convertValue(arr));
                    }
                    return bsonArray;
                case BLOB:
                    return new BsonBinary(((BlobValue) value).get().getData());
                case LONG:
                    return new BsonInt64(((LongValue) value).get());
                case DOUBLE:
                    return new BsonDouble(((DoubleValue) value).get());
                case ENTITY:
                    return convertFullEntity(((EntityValue) value).get());
                case BOOLEAN:
                    return new BsonBoolean(((BooleanValue) value).get());
                default:
                    throw new StoreException("Cannot convert Value: " + value.toString());
            }
        }

    }
}
