package com.cloudaware.store.model;

import java.util.Arrays;

/**
 * Generic binary holder.
 */
public class Binary {

    private final byte[] data;

    /**
     * Creates a Binary object
     *
     * @param data raw data
     */
    public Binary(final byte[] data) {
        this.data = data.clone();
    }

    /**
     * Get a copy of the binary value.
     *
     * @return a copy of the binary value.
     */
    public byte[] getData() {
        return data.clone();
    }

    /**
     * Get the length of the data.
     *
     * @return the length of the binary array.
     */
    public int length() {
        return data.length;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final Binary binary = (Binary) o;

        if (!Arrays.equals(data, binary.data)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        final int result = Arrays.hashCode(data);
        return result;
    }
}
