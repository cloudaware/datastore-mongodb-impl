package com.cloudaware.store;

public class StoreException extends RuntimeException {

    public StoreException(final String message) {
        super(message);
    }

    /**
     * Throw a DatastoreException with {@code FAILED_PRECONDITION} reason and the {@code message} in a
     * nested exception.
     *
     * @throws StoreException every time
     */
    public static StoreException throwInvalidRequest(final String massage, final Object... params) {
        throw new StoreException(String.format(massage, params));
    }
}
