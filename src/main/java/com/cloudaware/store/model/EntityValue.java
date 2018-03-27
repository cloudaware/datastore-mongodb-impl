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

public class EntityValue extends Value<FullEntity<?>> {

    public EntityValue(final FullEntity<?> entity) {
        this(newBuilder(entity));
    }

    private EntityValue(final Builder builder) {
        super(builder);
    }

    public static EntityValue of(final FullEntity<?> entity) {
        return new EntityValue(entity);
    }

    public static Builder newBuilder(final FullEntity<?> entity) {
        return new Builder().set(entity);
    }

    @Override
    public Builder toBuilder() {
        return new Builder().mergeFrom(this);
    }

    public static final class Builder extends Value.BaseBuilder<FullEntity<?>, EntityValue, Builder> {

        private Builder() {
            super(ValueType.ENTITY);
        }

        @Override
        public EntityValue build() {
            return new EntityValue(this);
        }
    }
}
