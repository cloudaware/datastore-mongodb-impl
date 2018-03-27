package com.cloudaware.store.model;

public enum ValueType {
    /**
     * Represents a {@code null} value.
     */
    NULL,

    /**
     * Represents a {@code string} value.
     */
    STRING,

    /**
     * Represents an entity value.
     */
    ENTITY,

    /**
     * Represents a {@code list} of {@link Value}s.
     */
    LIST,

    /**
     * Represents a {@code key} as a value.
     */
    KEY,

    /**
     * Represents a {@code long} value.
     */
    LONG,

    /**
     * Represents a {@code double} value.
     */
    DOUBLE,

    /**
     * Represents a {@code boolean} value.
     */
    BOOLEAN,

    /**
     * Represents a {@link com.cloudaware.store.model.Timestamp} value.
     */
    TIMESTAMP,

    /**
     * Represents a {@link Binary} value.
     */
    BLOB

    /**
     * Represents a raw/unparsed value.
     */
//  RAW_VALUE(com.google.cloud.datastore.ValueType.RAW_VALUE),

    /**
     * Represents a {@link LatLng} value.
     */
//  LAT_LNG(com.google.cloud.datastore.ValueType.LAT_LNG, BsonType.)

}
