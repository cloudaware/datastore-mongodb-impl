package com.cloudaware.store;

import org.junit.Test;

public class StoreExceptionTest {

    @Test(expected = StoreException.class)
    public void testCreation() {
        throw new StoreException("message");
    }

    @Test(expected = StoreException.class)
    public void testStaticCreation() {
        throw StoreException.throwInvalidRequest("test");
    }

}
