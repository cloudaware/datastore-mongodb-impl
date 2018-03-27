package com.cloudaware.store.env;

import com.cloudaware.store.StoreService;

import java.io.IOException;

public interface Backend {
    void setUp() throws Exception;

    void tearDown() throws Exception;

    StoreService getStoreService() throws Exception;

}
