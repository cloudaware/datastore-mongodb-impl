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

public final class KeyValue extends Value<Key> {

    public KeyValue(final Key key) {
        this(newBuilder(key));
    }

    private KeyValue(final Builder builder) {
        super(builder);
    }

    public static KeyValue of(final Key key) {
        return new KeyValue(key);
    }

    public static Builder newBuilder(final Key key) {
        return new Builder().set(key);
    }

    @Override
    public Builder toBuilder() {
        return new Builder().mergeFrom(this);
    }

    public static final class Builder extends Value.BaseBuilder<Key, KeyValue, Builder> {

        public Builder() {
            super(ValueType.KEY);
        }

        @Override
        public KeyValue build() {
            return new KeyValue(this);
        }
    }
}
