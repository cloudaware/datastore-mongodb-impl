package com.cloudaware.store.model;

public final class StringValue extends Value<String> {
    public StringValue(final String value) {
        this(newBuilder(value));
    }

    private StringValue(final Builder builder) {
        super(builder);
    }

    public static StringValue of(final String value) {
        return new StringValue(value);
    }

    public static Builder newBuilder(final String value) {
        return new Builder().set(value);
    }

    @Override
    public Builder toBuilder() {
        return new Builder().mergeFrom(this);
    }

    public static final class Builder extends Value.BaseBuilder<String, StringValue, Builder> {

        private Builder() {
            super(ValueType.STRING);
        }

        @Override
        public StringValue build() {
            return new StringValue(this);
        }
    }
}
