package com.cloudaware.store.env;

import com.cloudaware.store.BaseStoreServiceTest;
import com.cloudaware.store.StoreService;
import com.cloudaware.store.mongodb.MongoStoreService;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.mongo.tests.MongodForTestsFactory;

public class MongoBackend implements Backend {

    private MongodForTestsFactory mongoForTestFactory;

    @Override
    public void setUp() throws Exception {
        mongoForTestFactory = MongodForTestsFactory.with(Version.Main.V3_3);
    }

    @Override
    public void tearDown() throws Exception {
        mongoForTestFactory.shutdown();
    }

    @Override
    public StoreService getStoreService() throws Exception {
        return new MongoStoreService(mongoForTestFactory.newMongo(), BaseStoreServiceTest.DEFAULT_PROJECT_ID);
    }
}
