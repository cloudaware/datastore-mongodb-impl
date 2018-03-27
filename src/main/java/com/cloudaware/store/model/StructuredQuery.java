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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import static com.cloudaware.store.model.BlobValue.of;
import static com.cloudaware.store.model.BooleanValue.of;
import static com.cloudaware.store.model.DoubleValue.of;
import static com.cloudaware.store.model.KeyValue.of;
import static com.cloudaware.store.model.LongValue.of;
import static com.cloudaware.store.model.StringValue.of;
import static com.cloudaware.store.model.TimestampValue.of;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An implementation of a Google Cloud Datastore Query that can be constructed by providing
 * all the specific query elements.
 * <p>
 * <h3>A usage example:</h3>
 * <p>
 * <p>A simple query that returns all entities for a specific kind
 * <pre> {@code
 * Query<Entity> query = Query.newEntityQueryBuilder().setKind(kind).build();
 * QueryResults<Entity> results = datastore.run(query);
 * while (results.hasNext()) {
 *   Entity entity = results.next();
 *   ...
 * }
 * }</pre>
 * <p>
 * <p>A simple key-only query of all entities for a specific kind
 * <pre> {@code
 * Query<Key> keyOnlyQuery =  Query.newKeyQueryBuilder().setKind(KIND1).build();
 * QueryResults<Key> results = datastore.run(keyOnlyQuery);
 * ...
 * }</pre>
 * <p>
 * <p>A less trivial example of a projection query that returns the first 10 results
 * of "age" and "name" properties (sorted and grouped by "age") with an age greater than 18
 * <pre> {@code
 * Query<ProjectionEntity> query = Query.newProjectionEntityQueryBuilder()
 *     .setKind(kind)
 *     .setProjection(Projection.property("age"), Projection.first("name"))
 *     .setFilter(PropertyFilter.gt("age", 18))
 *     .setGroupBy("age")
 *     .setOrderBy(OrderBy.asc("age"))
 *     .setLimit(10)
 *     .build();
 * QueryResults<ProjectionEntity> results = datastore.run(query);
 * ...
 * }</pre>
 *
 * @param <V> the type of the result values this query will produce
 * @see <a href="https://cloud.google.com/appengine/docs/java/datastore/queries">Datastore
 * queries</a>
 */
public abstract class StructuredQuery<V> extends Query<V> {

    static final String KEY_PROPERTY_NAME = "__key__";
    private static final long serialVersionUID = 546838955624019594L;
    private final String kind;
    private final ImmutableList<String> projection;
    private final Filter filter;
    private final ImmutableList<String> distinctOn;
    private final ImmutableList<OrderBy> orderBy;
    private final int offset;
    private final Integer limit;

    StructuredQuery(final BuilderImpl<V, ?> builder) {
        super(builder.resultType, builder.projectId, builder.namespace);
        kind = builder.kind;
        projection = ImmutableList.copyOf(builder.projection);
        filter = builder.filter;
        distinctOn = ImmutableList.copyOf(builder.distinctOn);
        orderBy = ImmutableList.copyOf(builder.orderBy);
        offset = builder.offset;
        limit = builder.limit;
    }

    @Override
    public String toString() {
        return toStringHelper()
                .add("kind", kind)
                .add("offset", offset)
                .add("limit", limit)
                .add("filter", filter)
                .add("orderBy", orderBy)
                .add("projection", projection)
                .add("distinctOn", distinctOn)
                .toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getNamespace(), kind, offset, limit, filter,
                orderBy, projection, distinctOn);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof StructuredQuery)) {
            return false;
        }
        final StructuredQuery<?> other = (StructuredQuery<?>) obj;
        return Objects.equals(getNamespace(), other.getNamespace())
                && Objects.equals(kind, other.kind)
                && Objects.equals(offset, other.offset)
                && Objects.equals(limit, other.limit)
                && Objects.equals(filter, other.filter)
                && Objects.equals(orderBy, other.orderBy)
                && Objects.equals(projection, other.projection)
                && Objects.equals(distinctOn, other.distinctOn);

    }

    /**
     * Returns the kind for this query.
     */
    public String getKind() {
        return kind;
    }

    /**
     * Return ttrue if query key only
     *
     * @return
     */
    final boolean isKeyOnly() {
        return projection.size() == 1 && KEY_PROPERTY_NAME.equals(projection.get(0));
    }

    /**
     * Returns the projection for this query.
     */
    public List<String> getProjection() {
        return projection;
    }

    /**
     * Returns the filter for this query.
     */
    public Filter getFilter() {
        return filter;
    }

    /**
     * Returns the distinct on clause for this query.
     */
    public List<String> getDistinctOn() {
        return distinctOn;
    }

    /**
     * Returns the order by clause for this query.
     */
    public List<OrderBy> getOrderBy() {
        return orderBy;
    }

    /**
     * Returns the offset for this query.
     */
    public int getOffset() {
        return offset;
    }

    /**
     * Returns the limit for this query.
     */
    public Integer getLimit() {
        return limit;
    }

    public abstract Builder<V> toBuilder();

    /**
     * Interface for StructuredQuery builders.
     *
     * @param <V> the type of result the query returns.
     */
    public interface Builder<V> {

        /**
         * Sets the namespace for the query.
         */
        Builder<V> setNamespace(String namespace);

        /**
         * Sets the namespace for the query.
         */
        Builder<V> setProjectId(String projectId);

        /**
         * Sets the kind for the query.
         */
        Builder<V> setKind(String kind);

        /**
         * Sets the offset for the query.
         */
        Builder<V> setOffset(int offset);

        /**
         * Sets the limit for the query.
         */
        Builder<V> setLimit(Integer limit);

        Builder<V> setFilter(Filter filter);

        /**
         * Clears any previously specified order by settings.
         */
        Builder<V> clearOrderBy();

        /**
         * Sets the query's order by clause (clearing any previously specified order by settings).
         */
        Builder<V> setOrderBy(OrderBy orderBy, OrderBy... others);

        /**
         * Adds settings to the existing order by clause.
         */
        Builder<V> addOrderBy(OrderBy orderBy, OrderBy... others);

        StructuredQuery<V> build();
    }

    public abstract static class Filter implements Serializable {

        private static final long serialVersionUID = -6443285436239990860L;

        Filter() {
        }

    }

    /**
     * A class representing a filter composed of a combination of other filters.
     */
    public static final class CompositeFilter extends Filter {

        private static final long serialVersionUID = 3610352685739360009L;
        private final Operator operator;
        private final ImmutableList<Filter> filters;

        private CompositeFilter(final Operator operator, final Filter first, final Filter... other) {
            this.operator = operator;
            this.filters =
                    ImmutableList.<Filter>builder().add(first).addAll(Arrays.asList(other)).build();
        }

        private CompositeFilter(final Operator operator, final ImmutableList<Filter> filters) {
            this.operator = operator;
            this.filters = filters;
            Preconditions.checkArgument(!filters.isEmpty(), "filters list must not be empty");
        }

        public static CompositeFilter and(final Filter first, final Filter... other) {
            return new CompositeFilter(Operator.AND, first, other);
        }

        public Operator getOperator() {
            return operator;
        }

        public ImmutableList<Filter> getFilters() {
            return filters;
        }

        @Override
        public String toString() {
            final ToStringHelper toStringHelper = MoreObjects.toStringHelper(this);
            toStringHelper.add("operator", operator);
            toStringHelper.add("filters", filters);
            return toStringHelper.toString();
        }

        @Override
        public int hashCode() {
            return Objects.hash(operator, filters);
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof CompositeFilter)) {
                return false;
            }
            final CompositeFilter other = (CompositeFilter) obj;
            return operator.equals(other.operator)
                    && filters.equals(other.filters);
        }

        public enum Operator {
            AND
        }
    }

    /**
     * A class representing a filter based on a single property or ancestor.
     */
    public static final class PropertyFilter extends Filter {

        private static final long serialVersionUID = -4514695915258598597L;

        private final String property;
        private final Operator operator;
        private final Value<?> value;

        private PropertyFilter(final String property, final Operator operator, final Value<?> value) {
            this.property = checkNotNull(property);
            this.operator = checkNotNull(operator);
            this.value = checkNotNull(value);
        }

        public static PropertyFilter lt(final String property, final Value<?> value) {
            return new PropertyFilter(property, Operator.LESS_THAN, value);
        }

        public static PropertyFilter lt(final String property, final String value) {
            return new PropertyFilter(property, Operator.LESS_THAN, of(value));
        }

        public static PropertyFilter lt(final String property, final long value) {
            return new PropertyFilter(property, Operator.LESS_THAN, of(value));
        }

        public static PropertyFilter lt(final String property, final double value) {
            return new PropertyFilter(property, Operator.LESS_THAN, of(value));
        }

        public static PropertyFilter lt(final String property, final boolean value) {
            return new PropertyFilter(property, Operator.LESS_THAN, of(value));
        }

        public static PropertyFilter lt(final String property, final Timestamp value) {
            return new PropertyFilter(property, Operator.LESS_THAN, of(value));
        }

        public static PropertyFilter lt(final String property, final Key value) {
            return new PropertyFilter(property, Operator.LESS_THAN, of(value));
        }

        public static PropertyFilter lt(final String property, final Binary value) {
            return new PropertyFilter(property, Operator.LESS_THAN, of(value));
        }

        public static PropertyFilter le(final String property, final Value<?> value) {
            return new PropertyFilter(property, Operator.LESS_THAN_OR_EQUAL, value);
        }

        public static PropertyFilter le(final String property, final String value) {
            return new PropertyFilter(property, Operator.LESS_THAN_OR_EQUAL, of(value));
        }

        public static PropertyFilter le(final String property, final long value) {
            return new PropertyFilter(property, Operator.LESS_THAN_OR_EQUAL, of(value));
        }

        public static PropertyFilter le(final String property, final double value) {
            return new PropertyFilter(property, Operator.LESS_THAN_OR_EQUAL, of(value));
        }

        public static PropertyFilter le(final String property, final boolean value) {
            return new PropertyFilter(property, Operator.LESS_THAN_OR_EQUAL, of(value));
        }

        public static PropertyFilter le(final String property, final Timestamp value) {
            return new PropertyFilter(property, Operator.LESS_THAN_OR_EQUAL, of(value));
        }

        public static PropertyFilter le(final String property, final Key value) {
            return new PropertyFilter(property, Operator.LESS_THAN_OR_EQUAL, of(value));
        }

        public static PropertyFilter le(final String property, final Binary value) {
            return new PropertyFilter(property, Operator.LESS_THAN_OR_EQUAL, of(value));
        }

        public static PropertyFilter gt(final String property, final Value<?> value) {
            return new PropertyFilter(property, Operator.GREATER_THAN, value);
        }

        public static PropertyFilter gt(final String property, final String value) {
            return new PropertyFilter(property, Operator.GREATER_THAN, of(value));
        }

        public static PropertyFilter gt(final String property, final long value) {
            return new PropertyFilter(property, Operator.GREATER_THAN, of(value));
        }

        public static PropertyFilter gt(final String property, final double value) {
            return new PropertyFilter(property, Operator.GREATER_THAN, of(value));
        }

        public static PropertyFilter gt(final String property, final boolean value) {
            return new PropertyFilter(property, Operator.GREATER_THAN, of(value));
        }

        public static PropertyFilter gt(final String property, final Timestamp value) {
            return new PropertyFilter(property, Operator.GREATER_THAN, of(value));
        }

        public static PropertyFilter gt(final String property, final Key value) {
            return new PropertyFilter(property, Operator.GREATER_THAN, of(value));
        }

        public static PropertyFilter gt(final String property, final Binary value) {
            return new PropertyFilter(property, Operator.GREATER_THAN, of(value));
        }

        public static PropertyFilter ge(final String property, final Value<?> value) {
            return new PropertyFilter(property, Operator.GREATER_THAN_OR_EQUAL, value);
        }

        public static PropertyFilter ge(final String property, final String value) {
            return new PropertyFilter(property, Operator.GREATER_THAN_OR_EQUAL, of(value));
        }

        public static PropertyFilter ge(final String property, final long value) {
            return new PropertyFilter(property, Operator.GREATER_THAN_OR_EQUAL, of(value));
        }

        public static PropertyFilter ge(final String property, final double value) {
            return new PropertyFilter(property, Operator.GREATER_THAN_OR_EQUAL, of(value));
        }

        public static PropertyFilter ge(final String property, final boolean value) {
            return new PropertyFilter(property, Operator.GREATER_THAN_OR_EQUAL, of(value));
        }

        public static PropertyFilter ge(final String property, final Timestamp value) {
            return new PropertyFilter(property, Operator.GREATER_THAN_OR_EQUAL, of(value));
        }

        public static PropertyFilter ge(final String property, final Key value) {
            return new PropertyFilter(property, Operator.GREATER_THAN_OR_EQUAL, of(value));
        }

        public static PropertyFilter ge(final String property, final Binary value) {
            return new PropertyFilter(property, Operator.GREATER_THAN_OR_EQUAL, of(value));
        }

        public static PropertyFilter eq(final String property, final Value<?> value) {
            return new PropertyFilter(property, Operator.EQUAL, value);
        }

        public static PropertyFilter eq(final String property, final String value) {
            return new PropertyFilter(property, Operator.EQUAL, of(value));
        }

        public static PropertyFilter eq(final String property, final long value) {
            return new PropertyFilter(property, Operator.EQUAL, of(value));
        }

        public static PropertyFilter eq(final String property, final double value) {
            return new PropertyFilter(property, Operator.EQUAL, of(value));
        }

        public static PropertyFilter eq(final String property, final boolean value) {
            return new PropertyFilter(property, Operator.EQUAL, of(value));
        }

        public static PropertyFilter eq(final String property, final Timestamp value) {
            return new PropertyFilter(property, Operator.EQUAL, of(value));
        }

        public static PropertyFilter eq(final String property, final Key value) {
            return new PropertyFilter(property, Operator.EQUAL, of(value));
        }

        public static PropertyFilter eq(final String property, final Binary value) {
            return new PropertyFilter(property, Operator.EQUAL, of(value));
        }

        public static PropertyFilter isNull(final String property) {
            return new PropertyFilter(property, Operator.EQUAL, NullValue.of());
        }

        public String getProperty() {
            return property;
        }

        public Operator getOperator() {
            return operator;
        }

        public Value<?> getValue() {
            return value;
        }

        @Override
        public String toString() {
            final ToStringHelper toStringHelper = MoreObjects.toStringHelper(this);
            toStringHelper.add("property", property);
            toStringHelper.add("operator", operator);
            toStringHelper.add("value", value);
            return toStringHelper.toString();
        }

        @Override
        public int hashCode() {
            return Objects.hash(property, operator, value);
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof PropertyFilter)) {
                return false;
            }
            final PropertyFilter other = (PropertyFilter) obj;
            return property.equals(other.property)
                    && operator.equals(other.operator)
                    && Objects.equals(value, other.value);
        }

        public enum Operator {
            LESS_THAN,
            LESS_THAN_OR_EQUAL,
            GREATER_THAN,
            GREATER_THAN_OR_EQUAL,
            EQUAL,
            HAS_ANCESTOR
        }
    }

    public static final class OrderBy implements Serializable {

        private static final long serialVersionUID = 4091186784814400031L;

        private final String property;
        private final Direction direction;

        public OrderBy(final String property, final Direction direction) {
            this.property = checkNotNull(property);
            this.direction = checkNotNull(direction);
        }

        public static OrderBy asc(final String property) {
            return new OrderBy(property, OrderBy.Direction.ASCENDING);
        }

        public static OrderBy desc(final String property) {
            return new OrderBy(property, OrderBy.Direction.DESCENDING);
        }

        @Override
        public int hashCode() {
            return Objects.hash(property, direction);
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof OrderBy)) {
                return false;
            }
            final OrderBy other = (OrderBy) obj;
            return property.equals(other.property)
                    && direction.equals(other.direction);
        }

        /**
         * Returns the property according to which the query result should be ordered.
         */
        public String getProperty() {
            return property;
        }

        /**
         * Returns the order's direction.
         */
        public Direction getDirection() {
            return direction;
        }

        @Override
        public String toString() {
            final ToStringHelper toStringHelper = MoreObjects.toStringHelper(this);
            toStringHelper.add("property", getProperty());
            toStringHelper.add("direction", getDirection());
            return toStringHelper.toString();
        }

        public enum Direction {
            ASCENDING,
            DESCENDING
        }
    }

    /**
     * Base class for StructuredQuery builders.
     *
     * @param <V> the type of result the query returns.
     * @param <B> the query builder.
     */
    abstract static class BuilderImpl<V, B extends BuilderImpl<V, B>> implements Builder<V> {

        private final ResultType<V> resultType;
        private final List<String> projection = new LinkedList<>();
        private final List<String> distinctOn = new LinkedList<>();
        private final List<OrderBy> orderBy = new LinkedList<>();
        private String projectId;
        private String namespace;
        private String kind;
        private Filter filter;
        private int offset;
        private Integer limit;

        BuilderImpl(final ResultType<V> resultType) {
            this.resultType = resultType;
        }

        BuilderImpl(final StructuredQuery<V> query) {
            this(query.getType());
            namespace = query.getNamespace();
            projectId = query.getProjectId();
            kind = query.kind;
            projection.addAll(query.projection);
            filter = query.filter;
            distinctOn.addAll(query.distinctOn);
            orderBy.addAll(query.orderBy);
            offset = query.offset;
            limit = query.limit;
        }

        @SuppressWarnings("unchecked")
        B self() {
            return (B) this;
        }

        @Override
        public B setNamespace(final String namespaceArg) {
            this.namespace = namespaceArg;
            return self();
        }

        @Override
        public B setProjectId(final String projectIdArg) {
            this.projectId = projectIdArg;
            return self();
        }

        @Override
        public B setKind(final String kindArg) {
            this.kind = kindArg;
            return self();
        }

        @Override
        public B setOffset(final int offsetArg) {
            Preconditions.checkArgument(offsetArg >= 0, "offset must not be negative");
            this.offset = offsetArg;
            return self();
        }

        @Override
        public B setLimit(final Integer limitArg) {
            Preconditions.checkArgument(limitArg == null || limitArg > 0, "limit must be positive");
            this.limit = limitArg;
            return self();
        }

        @Override
        public B setFilter(final Filter filterArg) {
            this.filter = filterArg;
            return self();
        }

        @Override
        public B clearOrderBy() {
            orderBy.clear();
            return self();
        }

        @Override
        public B setOrderBy(final OrderBy orderByArg, final OrderBy... others) {
            clearOrderBy();
            addOrderBy(orderByArg, others);
            return self();
        }

        @Override
        public B addOrderBy(final OrderBy orderByArg, final OrderBy... others) {
            this.orderBy.add(orderByArg);
            Collections.addAll(this.orderBy, others);
            return self();
        }

        B clearProjection() {
            projection.clear();
            return self();
        }

        B setProjection(final String projectionArg, final String... others) {
            clearProjection();
            addProjection(projectionArg, others);
            return self();
        }

        B addProjection(final String projectionArg, final String... others) {
            this.projection.add(projectionArg);
            Collections.addAll(this.projection, others);
            return self();
        }

        B clearDistinctOn() {
            distinctOn.clear();
            return self();
        }

        B setDistinctOn(final String property, final String... others) {
            clearDistinctOn();
            addDistinctOn(property, others);
            return self();
        }

        B addDistinctOn(final String property, final String... others) {
            this.distinctOn.add(property);
            Collections.addAll(this.distinctOn, others);
            return self();
        }

        B mergeFrom(final StructuredQuery<?> queryPb) {
            if (queryPb.getKind() != null) {
                setKind(queryPb.getKind());
            }
            setOffset(queryPb.getOffset());
            if (queryPb.getLimit() != null) {
                setLimit(queryPb.getLimit());
            }
            if (queryPb.getFilter() != null) {
                final Filter currFilter = queryPb.getFilter();
                if (currFilter != null) {
                    setFilter(currFilter);
                }
            }
            for (final OrderBy orderByPb : queryPb.getOrderBy()) {
                addOrderBy(orderByPb);
            }
            for (final String projectionPb : queryPb.getProjection()) {
                addProjection(projectionPb);
            }
            for (final String distinctOnPb : queryPb.getDistinctOn()) {
                addDistinctOn(distinctOnPb);
            }
            return self();
        }
    }
}
