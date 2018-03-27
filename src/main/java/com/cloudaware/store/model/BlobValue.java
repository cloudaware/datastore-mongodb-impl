package com.cloudaware.store.model;

public final class BlobValue extends Value<Binary> {
    public BlobValue(final Binary blob) {
        this(newBuilder(blob));
    }

    private BlobValue(final Builder builder) {
        super(builder);
    }

    public static BlobValue of(final Binary blob) {
        return new BlobValue(blob);
    }

    public static Builder newBuilder(final Binary blob) {
        return new Builder().set(blob);
    }

    @Override
    public Builder toBuilder() {
        return new Builder().mergeFrom(this);
    }

    public static final class Builder extends Value.BaseBuilder<Binary, BlobValue, Builder> {

        private Builder() {
            super(ValueType.BLOB);
        }

        @Override
        public BlobValue build() {
            return new BlobValue(this);
        }
    }
}
