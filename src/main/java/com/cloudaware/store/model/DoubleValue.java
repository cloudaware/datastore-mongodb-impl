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

public final class DoubleValue extends Value<Double> {

    public DoubleValue(final double value) {
        this(newBuilder(value));
    }

    private DoubleValue(final Builder builder) {
        super(builder);
    }

    public static DoubleValue of(final double value) {
        return new DoubleValue(value);
    }

    public static Builder newBuilder(final double value) {
        return new Builder().set(value);
    }

    @Override
    public Builder toBuilder() {
        return new Builder().mergeFrom(this);
    }

    public static final class Builder extends Value.BaseBuilder<Double, DoubleValue, Builder> {

        public Builder() {
            super(ValueType.DOUBLE);
        }

        @Override
        public DoubleValue build() {
            return new DoubleValue(this);
        }
    }
}
