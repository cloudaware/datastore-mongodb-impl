package com.cloudaware.store;

import com.cloudaware.store.env.Backend;
import com.cloudaware.store.model.Binary;
import com.cloudaware.store.model.DoubleValue;
import com.cloudaware.store.model.Entity;
import com.cloudaware.store.model.EntityQuery;
import com.cloudaware.store.model.Key;
import com.cloudaware.store.model.KeyQuery;
import com.cloudaware.store.model.NullValue;
import com.cloudaware.store.model.QueryResults;
import com.cloudaware.store.model.StringValue;
import com.cloudaware.store.model.StructuredQuery;
import com.cloudaware.store.model.Timestamp;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

public class QueryOperation extends BaseStoreServiceTest {

    private static final String KIND = "test-query";

    private Binary bin;
    private Key paramKey;
    private Key childKey;
    private Entity childEntity;
    private Timestamp timestamp;

    public QueryOperation(final Backend backend) {
        super(backend);
    }

    @Test
    public void keyQuery() throws Exception {
        StoreService storeService = getStoreService();

        KeyQuery.Builder query = KeyQuery.newKeyQueryBuilder()
                .setKind(KIND)
                .setOffset(0)
                .setLimit(10)
                .setOrderBy(StructuredQuery.OrderBy.desc("num"), StructuredQuery.OrderBy.asc("timestamp"))
                .setFilter(StructuredQuery.PropertyFilter.ge("num", 0));

        QueryResults<Key> results = storeService.run(query.build());

        int count = 0;
        while (results.hasNext()) {
            Key key = results.next();
            Assert.assertEquals(KIND, key.getKind());
            count++;
        }
        Assert.assertEquals(5, count);
    }

    @Test
    public void entityQuery() throws Exception {
        StoreService storeService = getStoreService();

        EntityQuery.Builder query = KeyQuery.newEntityQueryBuilder()
                .setKind(KIND)
                .setOffset(0)
                .setLimit(10)
                .setOrderBy(StructuredQuery.OrderBy.desc("num"), StructuredQuery.OrderBy.asc("timestamp"))
                .setFilter(
                        StructuredQuery.CompositeFilter.and(
                                StructuredQuery.PropertyFilter.ge("num", 0)
                        )
                );

        QueryResults<Entity> results = storeService.run(query.build());

        int count = 0;
        while (results.hasNext()) {
            Entity entity = results.next();
            Assert.assertEquals(KIND, entity.getKey().getKind());
            Assert.assertEquals("test-string", entity.getString("stringSingular"));
            Assert.assertEquals(Lists.newArrayList(StringValue.of("test1"), StringValue.of("test2")), entity.getList("stringMultiple"));
            Assert.assertEquals(Long.MAX_VALUE, entity.getLong("long"));
            Assert.assertEquals(DoubleValue.of(1.2), entity.getValue("double"));
            Assert.assertEquals(true, entity.getBoolean("bool"));
            Assert.assertEquals(bin, entity.getBlob("bin"));
            Assert.assertEquals(NullValue.of(), entity.getValue("null"));
            Assert.assertEquals(paramKey, entity.getKey("paramKey"));
            Assert.assertEquals(childEntity, entity.getEntity("entity"));
            Assert.assertEquals(timestamp, entity.getTimestamp("staticTimestamp"));
            count++;
        }
        Assert.assertEquals(5, count);
    }

    @Override
    protected void setUp() {
        try {
            StoreService storeService = getStoreService();
            this.bin = new Binary("test-binary".getBytes());
            this.paramKey = nextNameKey("test-param-key");
            this.childKey = nextNameKey("test-child-key");
            this.childEntity = Entity.newBuilder(childKey)
                    .setNull("nullField")
                    .set("bin", bin)
                    .build();
            this.timestamp = Timestamp.now();

            for (int i = 0; i < 5; i++) {
                Entity entity = Entity.newBuilder(nextNameKey(KIND))
                        .set("stringSingular", "test-string")
                        .set("stringMultiple", "test1", "test2")
                        .set("long", Long.MAX_VALUE)
                        .set("num", i)
                        .set("double", new Double(1.2))
                        .set("bool", true)
                        .set("bin", bin)
                        .set("null", NullValue.of())
                        .set("paramKey", paramKey)
                        .set("entity", childEntity)
                        .set("timestamp", Timestamp.now())
                        .set("staticTimestamp", timestamp)
                        .build();
                Entity entity1 = storeService.put(entity);
                Assert.assertEquals(entity1.getKey(), entity.getKey());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void tearDown() {
        this.bin = null;
        this.timestamp = null;
        this.childEntity = null;
        this.childKey = null;
        this.paramKey = null;
    }
}
