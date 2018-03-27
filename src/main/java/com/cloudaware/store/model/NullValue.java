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

public final class NullValue extends Value<Void> {

    public NullValue() {
        this(newBuilder());
    }

    private NullValue(final Builder builder) {
        super(builder);
    }

    public static NullValue of() {
        return new NullValue();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public Builder toBuilder() {
        return new Builder().mergeFrom(this);
    }

    public static final class Builder extends Value.BaseBuilder<Void, NullValue, Builder> {

        private Builder() {
            super(ValueType.NULL);
        }

        @Override
        public NullValue build() {
            return new NullValue(this);
        }

        @Override
        public Builder set(final Void value) {
            Preconditions.checkArgument(value == null, "Only null values are allowed");
            return this;
        }
    }
}
