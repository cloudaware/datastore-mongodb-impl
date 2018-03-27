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

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;

import java.io.Serializable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A Google Cloud Datastore query.
 * For usage examples see {@link StructuredQuery}.
 * <p>
 * <p>Note that queries require proper indexing. See
 * <a href="https://cloud.google.com/datastore/docs/tools/indexconfig">
 * Cloud Datastore Index Configuration</a> for help configuring indexes.
 *
 * @param <V> the type of the values returned by this query.
 * @see <a href="https://cloud.google.com/datastore/docs/concepts/queries">Datastore Queries</a>
 */
public abstract class Query<V> implements Serializable {

    private static final long serialVersionUID = 7967659059395653941L;

    private final ResultType<V> resultType;
    private final String namespace;
    private final String projectId;

    Query(final ResultType<V> resultType, final String namespace, final String projectId) {
        this.resultType = checkNotNull(resultType);
        this.namespace = namespace;
        this.projectId = projectId;
    }

    /**
     * Returns a new {@link StructuredQuery} builder for full (complete entities) queries.
     * <p>
     * <p>Example of creating and running an entity query.
     * <pre> {@code
     * String kind = "my_kind";
     * Query<Entity> query = Query.newEntityQueryBuilder().setKind(kind).build();
     * QueryResults<Entity> results = datastore.run(query);
     * // Use results
     * }</pre>
     */
    public static EntityQuery.Builder newEntityQueryBuilder() {
        return new EntityQuery.Builder();
    }

    /**
     * Returns a new {@link StructuredQuery} builder for key only queries.
     * <p>
     * <p>Example of creating and running a key query.
     * <pre> {@code
     * String kind = "my_kind";
     * Query<Key> query = Query.newKeyQueryBuilder().setKind(kind).build();
     * QueryResults<Key> results = datastore.run(query);
     * // Use results
     * }</pre>
     */
    public static KeyQuery.Builder newKeyQueryBuilder() {
        return new KeyQuery.Builder();
    }

    /**
     * Returns a new {@link StructuredQuery} builder for projection queries.
     * <p>
     * <p>Example of creating and running a projection entity query.
     * <pre> {@code
     * String kind = "my_kind";
     * String property = "my_property";
     * Query<ProjectionEntity> query = Query.newProjectionEntityQueryBuilder()
     *     .setKind(kind)
     *     .addProjection(property)
     *     .build();
     * QueryResults<ProjectionEntity> results = datastore.run(query);
     * // Use results
     * }</pre>
     */
    public static ProjectionEntityQuery.Builder newProjectionEntityQueryBuilder() {
        return new ProjectionEntityQuery.Builder();
    }

    /**
     * get type
     *
     * @return
     */
    public ResultType<V> getType() {
        return resultType;
    }

    /**
     * get namespace
     *
     * @return
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * get projectId
     *
     * @return
     */
    public String getProjectId() {
        return projectId;
    }

    /**
     * get String helper
     *
     * @return
     */
    final ToStringHelper toStringHelper() {
        return MoreObjects.toStringHelper(this).add("type", resultType).add("namespace", namespace).add("projectId", projectId);
    }

    /**
     * This class represents the expected type of the result.
     * ENTITY: A full entity represented by {@link Entity}.
     * PROJECTION_ENTITY: A projection entity, represented by {@link ProjectionEntity}.
     * KEY: An entity's {@link Key}.
     */
    public abstract static class ResultType<V> implements Serializable {

        public static final ResultType<Entity> ENTITY =
                new ResultType<Entity>(Entity.class) {
                    private static final long serialVersionUID = 7712959777507168274L;
                };
        public static final ResultType<Key> KEY =
                new ResultType<Key>(Key.class) {

                    private static final long serialVersionUID = -8514289244104446252L;
                };
        public static final ResultType<ProjectionEntity> PROJECTION_ENTITY =
                new ResultType<ProjectionEntity>(ProjectionEntity.class) {

                    private static final long serialVersionUID = -7591409419690650246L;

                };
        static final ResultType<?> UNKNOWN = new ResultType<Object>(Object.class) {

            private static final long serialVersionUID = 1602329532153860907L;

        };
        private static final long serialVersionUID = 2104157695425806623L;
        //        private static final Map<com.google.datastore.v1.EntityResult.ResultType, ResultType<?>> PB_TO_INSTANCE = Maps.newEnumMap(com.google.datastore.v1.EntityResult.ResultType.class);
        private final Class<V> resultClass;

        private ResultType(final Class<V> resultClass) {
            this.resultClass = resultClass;
//            if (queryType != null) {
//                PB_TO_INSTANCE.put(queryType, this);
//            }
        }

        public Class<V> resultClass() {
            return resultClass;
        }

        @Override
        public int hashCode() {
            return resultClass.hashCode();
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof ResultType)) {
                return false;
            }
            final ResultType<?> other = (ResultType<?>) obj;
            return resultClass.equals(other.resultClass);
        }

        @Override
        public String toString() {
            final ToStringHelper toStringHelper = MoreObjects.toStringHelper(this);
            toStringHelper.add("resultClass", resultClass);
            return toStringHelper.toString();
        }

        boolean isAssignableFrom(final ResultType<?> otherResultType) {
            return resultClass.isAssignableFrom(otherResultType.resultClass);
        }

    }
}
