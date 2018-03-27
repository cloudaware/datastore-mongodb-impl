package com.cloudaware.store.env;

import com.cloudaware.store.BaseStoreServiceTest;
import com.cloudaware.store.StoreService;
import com.cloudaware.store.datastore.DatastoreService;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.testing.LocalDatastoreHelper;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class DatastoreBackend implements Backend {
    private static LocalDatastoreHelper localDatastoreHelper = LocalDatastoreHelper.create(1.0);
    private static Datastore datastore;

    public static void beforeClass() {
        try {
            localDatastoreHelper.start();
            System.setProperty(com.google.datastore.v1.client.DatastoreHelper.LOCAL_HOST_ENV_VAR, localDatastoreHelper.getOptions().getHost());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void afterClass() {
        try {
            localDatastoreHelper.stop(Duration.ofSeconds(2));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void setUp() throws Exception {
        localDatastoreHelper.reset();
        this.datastore = DatastoreOptions.newBuilder().setProjectId(BaseStoreServiceTest.DEFAULT_PROJECT_ID).setHost(localDatastoreHelper.getOptions().getHost()).build().getService();
    }

    @Override
    public void tearDown() throws Exception {
    }

    @Override
    public StoreService getStoreService() throws Exception {
        return new DatastoreService(this.datastore);
    }
}
