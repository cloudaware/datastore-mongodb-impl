package com.cloudaware.store;

import com.cloudaware.store.env.Backend;
import com.cloudaware.store.env.DatastoreBackend;
import com.cloudaware.store.env.MongoBackend;
import com.cloudaware.store.model.Key;
import com.cloudaware.store.model.KeyFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(Parameterized.class)
public abstract class BaseStoreServiceTest {

    public static String DEFAULT_PROJECT_ID = "test-project";
    private Map<String, AtomicInteger> keyFactory = Maps.<String, AtomicInteger>newConcurrentMap();
    private Backend backend;

    public BaseStoreServiceTest(final Backend backend) {
        this.backend = backend;
    }

    @Parameterized.Parameters
    public static Collection<Backend> getBackends() {
        return ImmutableList.of(new MongoBackend(), new DatastoreBackend());
    }

    @BeforeClass
    public static void beforeClass() {
        DatastoreBackend.beforeClass();
    }

    @AfterClass
    public static void afterClass() {
        DatastoreBackend.afterClass();
    }

    @Before
    public void baseSetUp() throws Exception {
        backend.setUp();
        setUp();
    }

    @After
    public void baseTearDown() throws Exception {
        tearDown();
        backend.tearDown();
    }

    protected void setUp() {
    }

    protected void tearDown() {
    }

    protected Key nextIdKey(final String kind) throws Exception {
        if (!keyFactory.containsKey(kind)) {
            keyFactory.put(kind, new AtomicInteger(1));
        }
        AtomicInteger atomicInteger = keyFactory.get(kind);
        return getKeyFactory(kind).newKey(atomicInteger.getAndIncrement());
    }

    protected Key nextNameKey(final String kind) throws Exception {
        if (!keyFactory.containsKey(kind)) {
            keyFactory.put(kind, new AtomicInteger(1));
        }
        AtomicInteger atomicInteger = keyFactory.get(kind);
        return getKeyFactory(kind).newKey("test-name-" + atomicInteger.getAndIncrement());
    }

    protected KeyFactory getKeyFactory(final String kind) throws Exception {
        return getStoreService().newKeyFactory().setKind(kind);
    }

    protected StoreService getStoreService() throws Exception {
        return backend.getStoreService();
    }

}
