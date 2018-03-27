/*
 * Copyright 2015 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudaware.store.model;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * A Google Cloud Datastore list value. A list value is a list of {@link Value} objects.
 */
public final class ListValue extends Value<List<? extends Value<?>>> {

    public ListValue(final List<? extends Value<?>> values) {
        this(newBuilder().set(values));
    }

    public ListValue(final Value<?> first, final Value<?>... other) {
        this(new Builder().addValue(first, other));
    }

    private ListValue(final Builder builder) {
        super(builder);
    }

    /**
     * Creates a {@code ListValue} object given a list of {@code Value} objects.
     */
    public static ListValue of(final List<? extends Value<?>> values) {
        return new ListValue(values);
    }

    /**
     * Creates a {@code ListValue} object given a number of {@code Value} objects.
     */
    public static ListValue of(final Value<?> first, final Value<?>... other) {
        return new ListValue(first, other);
    }

    /**
     * Creates a {@code ListValue} object given a number of string values.
     */
    public static ListValue of(final String first, final String... other) {
        return newBuilder().addValue(first, other).build();
    }

    /**
     * Creates a {@code ListValue} object given a number of long values.
     */
    public static ListValue of(final long first, final long... other) {
        return newBuilder().addValue(first, other).build();
    }

    /**
     * Creates a {@code ListValue} object given a number of double values.
     */
    public static ListValue of(final double first, final double... other) {
        return newBuilder().addValue(first, other).build();
    }

    /**
     * Creates a {@code ListValue} object given a number of boolean values.
     */
    public static ListValue of(final boolean first, final boolean... other) {
        return newBuilder().addValue(first, other).build();
    }

    /**
     * Creates a {@code ListValue} object given a number of {@code Timestamp} values.
     */
    public static ListValue of(final Timestamp first, final Timestamp... other) {
        return newBuilder().addValue(first, other).build();
    }

    /**
     * Creates a {@code ListValue} object given a number of {@code Key} values.
     */
    public static ListValue of(final Key first, final Key... other) {
        return newBuilder().addValue(first, other).build();
    }

    /**
     * Creates a {@code ListValue} object given a number of {@code FullEntity} values.
     */
    public static ListValue of(final FullEntity<?> first, final FullEntity<?>... other) {
        return newBuilder().addValue(first, other).build();
    }

//  /**
//   * Creates a {@code ListValue} object given a number of {@code LatLng} values.
//   */
//  public static ListValue of(LatLng first, LatLng... other) {
//    return newBuilder().addValue(first, other).build();
//  }

    /**
     * Creates a {@code ListValue} object given a number of {@code Blob} values.
     */
    public static ListValue of(final Binary first, final Binary... other) {
        return newBuilder().addValue(first, other).build();
    }

    /**
     * Returns a builder for {@code ListValue} objects.
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Returns a builder for the list value object.
     */
    @Override
    public Builder toBuilder() {
        return new Builder().mergeFrom(this);
    }

    public static final class Builder extends
            Value.BaseBuilder<List<? extends Value<?>>, ListValue, Builder> {

        private ImmutableList.Builder<Value<?>> listBuilder = ImmutableList.builder();

        private Builder() {
            super(ValueType.LIST);
        }

        private void addValueHelper(final Value<?> value) {
            // see datastore.proto definition for list_value
            Preconditions.checkArgument(value.getType() != ValueType.LIST, "Cannot contain another list");
            listBuilder.add(value);
        }

        /**
         * Adds the provided values to the {@code ListValue} builder.
         */
        public Builder addValue(final Value<?> first, final Value<?>... other) {
            addValueHelper(first);
            for (final Value<?> value : other) {
                addValueHelper(value);
            }
            return this;
        }

        /**
         * Adds the provided string values to the {@code ListValue} builder.
         */
        public Builder addValue(final String first, final String... other) {
            listBuilder.add(StringValue.of(first));
            for (final String value : other) {
                listBuilder.add(StringValue.of(value));
            }
            return this;
        }

        /**
         * Adds the provided long values to the {@code ListValue} builder.
         */
        public Builder addValue(final long first, final long... other) {
            listBuilder.add(LongValue.of(first));
            for (final long value : other) {
                listBuilder.add(LongValue.of(value));
            }
            return this;
        }

        /**
         * Adds the provided double values to the {@code ListValue} builder.
         */
        public Builder addValue(final double first, final double... other) {
            listBuilder.add(DoubleValue.of(first));
            for (final double value : other) {
                listBuilder.add(DoubleValue.of(value));
            }
            return this;
        }

        /**
         * Adds the provided boolean values to the {@code ListValue} builder.
         */
        public Builder addValue(final boolean first, final boolean... other) {
            listBuilder.add(BooleanValue.of(first));
            for (final boolean value : other) {
                listBuilder.add(BooleanValue.of(value));
            }
            return this;
        }

        /**
         * Adds the provided {@code Timestamp} values to the {@code ListValue} builder.
         */
        public Builder addValue(final Timestamp first, final Timestamp... other) {
            listBuilder.add(TimestampValue.of(first));
            for (final Timestamp value : other) {
                listBuilder.add(TimestampValue.of(value));
            }
            return this;
        }

//    /**
//     * Adds the provided {@code LatLng} values to the {@code ListValue} builder.
//     */
//    public Builder addValue(LatLng first, LatLng... other) {
//      listBuilder.add(LatLngValue.of(first));
//      for (LatLng value : other) {
//        listBuilder.add(LatLngValue.of(value));
//      }
//      return this;
//    }

        /**
         * Adds the provided {@code Key} values to the {@code ListValue} builder.
         */
        public Builder addValue(final Key first, final Key... other) {
            listBuilder.add(KeyValue.of(first));
            for (final Key value : other) {
                listBuilder.add(KeyValue.of(value));
            }
            return this;
        }

        /**
         * Adds the provided {@code FullEntity} values to the {@code ListValue} builder.
         */
        public Builder addValue(final FullEntity<?> first, final FullEntity<?>... other) {
            listBuilder.add(EntityValue.of(first));
            for (final FullEntity<?> value : other) {
                listBuilder.add(EntityValue.of(value));
            }
            return this;
        }

        /**
         * Adds the provided {@code Blob} values to the {@code ListValue} builder.
         */
        public Builder addValue(final Binary first, final Binary... other) {
            listBuilder.add(BlobValue.of(first));
            for (final Binary value : other) {
                listBuilder.add(BlobValue.of(value));
            }
            return this;
        }

        /**
         * Sets the list of values of this {@code ListValue} builder to {@code values}. The provided
         * list is copied.
         *
         * @see BaseBuilder#set(Object)
         */
        @Override
        public Builder set(final List<? extends Value<?>> values) {
            listBuilder = ImmutableList.builder();
            for (final Value<?> value : values) {
                addValue(value);
            }
            return this;
        }

        @Override
        public List<? extends Value<?>> get() {
            return listBuilder.build();
        }

        /**
         * Creates a {@code ListValue} object.
         */
        @Override
        public ListValue build() {
            return new ListValue(this);
        }
    }
}
