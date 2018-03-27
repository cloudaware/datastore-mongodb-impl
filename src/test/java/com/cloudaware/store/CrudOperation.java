package com.cloudaware.store;

import com.cloudaware.store.env.Backend;
import com.cloudaware.store.model.Binary;
import com.cloudaware.store.model.DoubleValue;
import com.cloudaware.store.model.Entity;
import com.cloudaware.store.model.FullEntity;
import com.cloudaware.store.model.Key;
import com.cloudaware.store.model.KeyFactory;
import com.cloudaware.store.model.NullValue;
import com.cloudaware.store.model.StringValue;
import com.cloudaware.store.model.Timestamp;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

/**
 * Intergation test for crud operation over StoreService
 */
public class CrudOperation extends BaseStoreServiceTest {

    public CrudOperation(final Backend backend) {
        super(backend);
    }

    @Test
    public void put_get() throws Exception {
        StoreService storeService = getStoreService();
        Key key = nextNameKey("test-put_get");
        Key paramKey = nextNameKey("test-param-key");
        Key childKey = nextNameKey("test-child-key");
        Binary bin = new Binary("test-binary".getBytes());

        Entity childEntity = Entity.newBuilder(childKey)
                .setNull("null")
                .set("bin", bin)
                .build();
        Timestamp timestamp = Timestamp.now();
        Entity entity = Entity.newBuilder(key)
                .set("stringSingular", "test-string")
                .set("stringMultiple", "test1", "test2")
                .set("long", Long.MAX_VALUE)
                .set("double", new Double(1.2))
                .set("bool", true)
                .set("bin", bin)
                .set("null", NullValue.of())
                .set("paramKey", paramKey)
                .set("entity", childEntity)
                .set("timestamp", timestamp)
                .build();
        storeService.put(entity);

        Entity entityRes = storeService.get(key);

        Assert.assertEquals(entity, entityRes);
        Assert.assertEquals(entity.getString("stringSingular"), "test-string");
        Assert.assertEquals(entity.getList("stringMultiple"), Lists.newArrayList(StringValue.of("test1"), StringValue.of("test2")));
        Assert.assertEquals(entity.getLong("long"), Long.MAX_VALUE);
        Assert.assertEquals(entity.getValue("double"), DoubleValue.of(1.2));
        Assert.assertEquals(entity.getBoolean("bool"), true);
        Assert.assertEquals(entity.getBlob("bin"), bin);
        Assert.assertEquals(entity.getValue("null"), NullValue.of());
        Assert.assertEquals(entity.getKey("paramKey"), paramKey);
        Assert.assertEquals(entity.getEntity("entity"), childEntity);
        Assert.assertEquals(entity.getTimestamp("timestamp"), timestamp);

    }

    @Test
    public void delete() throws Exception {
        StoreService storeService = getStoreService();
        List<Entity> entityPut = Lists.newArrayList();
        List<Key> keysPut = Lists.newArrayList();
        for (int i = 0; i < 5; i++) {
            Key key = nextIdKey("test-delete");
            keysPut.add(key);
            Entity entity = Entity.newBuilder(key)
                    .set("string", "string")
                    .build();
            entityPut.add(entity);
        }
        storeService.put(entityPut);
        storeService.delete(keysPut.subList(0, 1));
        Iterator<Entity> entitiesGet = storeService.get(keysPut.subList(1, 4));
        while (entitiesGet.hasNext()) {
            Assert.assertTrue(keysPut.subList(1, 4).contains(entitiesGet.next().getKey()));
        }

        Entity entity = storeService.get(keysPut.get(0));
        Assert.assertNull(entity);
    }

    @Test
    public void create() throws Exception {
        StoreService storeService = getStoreService();
        KeyFactory keyFactory = getKeyFactory("test-create");
        FullEntity fullEntity = FullEntity.newBuilder(keyFactory.newKey()).build();
        //todo: implement add method;
//        storeService.add()

    }
}
