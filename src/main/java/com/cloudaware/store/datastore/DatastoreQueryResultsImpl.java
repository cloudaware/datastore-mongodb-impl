package com.cloudaware.store.datastore;

import com.cloudaware.store.model.EntityQuery;
import com.cloudaware.store.model.KeyQuery;
import com.cloudaware.store.model.ProjectionEntityQuery;
import com.cloudaware.store.model.Query;
import com.cloudaware.store.model.QueryResults;
import com.cloudaware.store.model.StructuredQuery;

import java.util.stream.Collectors;

public class DatastoreQueryResultsImpl<T> implements QueryResults<T> {

    private final DatastoreService datastoreService;
    private final Query<T> query;
    private final com.google.cloud.datastore.QueryResults queryResult;

    public DatastoreQueryResultsImpl(final DatastoreService datastoreService, final Query<T> query) {
        this.query = query;
        this.datastoreService = datastoreService;
        queryResult = this.datastoreService.getDatastore().run(convertQuery(query));

    }

    @Override
    public boolean hasNext() {
        return this.queryResult.hasNext();
    }

    @Override
    @SuppressWarnings("unchecked")
    public T next() {
        final T result;
        if (Query.ResultType.ENTITY.equals(query.getType())) {
            result = (T) DatastoreService.UNMARSHALLER.convertEntity((com.google.cloud.datastore.Entity) this.queryResult.next());
        } else if (Query.ResultType.KEY.equals(query.getType())) {
            result = (T) DatastoreService.UNMARSHALLER.convertKey((com.google.cloud.datastore.Key) this.queryResult.next());
        } else if (Query.ResultType.PROJECTION_ENTITY.equals(query.getType())) {
            result = (T) DatastoreService.UNMARSHALLER.convertProjectionEntity((com.google.cloud.datastore.ProjectionEntity) this.queryResult.next());
        } else {
            throw new UnsupportedOperationException();
        }
        return result;
    }

    private com.google.cloud.datastore.Query convertQuery(final Query<T> queryArg) {
        if (Query.ResultType.KEY.equals(queryArg.getType())) {
            final KeyQuery keyQuery = (KeyQuery) queryArg;
            final com.google.cloud.datastore.KeyQuery.Builder builder = com.google.cloud.datastore.Query.newKeyQueryBuilder()
                    .setKind(keyQuery.getKind())
                    .setNamespace(keyQuery.getNamespace())
                    .setLimit(keyQuery.getLimit())
                    .setOffset(keyQuery.getOffset());
            if (keyQuery.getOrderBy() != null && keyQuery.getOrderBy().size() > 0) {
                for (final StructuredQuery.OrderBy orderBy : keyQuery.getOrderBy()) {
                    if (orderBy.getDirection().equals(StructuredQuery.OrderBy.Direction.ASCENDING)) {
                        builder.addOrderBy(com.google.cloud.datastore.StructuredQuery.OrderBy.asc(orderBy.getProperty()));
                    } else if (orderBy.getDirection().equals(StructuredQuery.OrderBy.Direction.DESCENDING)) {
                        builder.addOrderBy(com.google.cloud.datastore.StructuredQuery.OrderBy.desc(orderBy.getProperty()));
                    }
                }
            }
            if (keyQuery.getFilter() != null) {
                builder.setFilter(convertFilter(keyQuery.getFilter()));
            }
            return builder.build();
        } else if (Query.ResultType.ENTITY.equals(queryArg.getType())) {
            final EntityQuery entityQuery = (EntityQuery) queryArg;
            final com.google.cloud.datastore.EntityQuery.Builder builder = com.google.cloud.datastore.Query.newEntityQueryBuilder()
                    .setKind(entityQuery.getKind())
                    .setNamespace(entityQuery.getNamespace())
                    .setLimit(entityQuery.getLimit())
                    .setOffset(entityQuery.getOffset());
            if (entityQuery.getOrderBy() != null && entityQuery.getOrderBy().size() > 0) {
                for (final StructuredQuery.OrderBy orderBy : entityQuery.getOrderBy()) {
                    if (orderBy.getDirection().equals(StructuredQuery.OrderBy.Direction.ASCENDING)) {
                        builder.addOrderBy(com.google.cloud.datastore.StructuredQuery.OrderBy.asc(orderBy.getProperty()));
                    } else if (orderBy.getDirection().equals(StructuredQuery.OrderBy.Direction.DESCENDING)) {
                        builder.addOrderBy(com.google.cloud.datastore.StructuredQuery.OrderBy.desc(orderBy.getProperty()));
                    }
                }
            }
            if (entityQuery.getFilter() != null) {
                builder.setFilter(convertFilter(entityQuery.getFilter()));
            }
            return builder.build();
        } else if (Query.ResultType.PROJECTION_ENTITY.equals(queryArg.getType())) {
            final ProjectionEntityQuery projectionEntityQuery = (ProjectionEntityQuery) queryArg;
            final com.google.cloud.datastore.ProjectionEntityQuery.Builder builder = com.google.cloud.datastore.Query.newProjectionEntityQueryBuilder()
                    .setKind(projectionEntityQuery.getKind())
                    .setNamespace(projectionEntityQuery.getNamespace())
                    .setLimit(projectionEntityQuery.getLimit())
                    .setOffset(projectionEntityQuery.getOffset());
            if (projectionEntityQuery.getOrderBy() != null && projectionEntityQuery.getOrderBy().size() > 0) {
                for (final StructuredQuery.OrderBy orderBy : projectionEntityQuery.getOrderBy()) {
                    if (orderBy.getDirection().equals(StructuredQuery.OrderBy.Direction.ASCENDING)) {
                        builder.addOrderBy(com.google.cloud.datastore.StructuredQuery.OrderBy.asc(orderBy.getProperty()));
                    } else if (orderBy.getDirection().equals(StructuredQuery.OrderBy.Direction.DESCENDING)) {
                        builder.addOrderBy(com.google.cloud.datastore.StructuredQuery.OrderBy.desc(orderBy.getProperty()));
                    }
                }
            }
            if (projectionEntityQuery.getFilter() != null) {
                builder.setFilter(convertFilter(projectionEntityQuery.getFilter()));
            }
            if (projectionEntityQuery.getProjection() != null) {
                if (projectionEntityQuery.getProjection().size() > 1) {
                    builder.setProjection(
                            projectionEntityQuery.getProjection().get(0),
                            projectionEntityQuery.getProjection().subList(1, projectionEntityQuery.getProjection().size()).toArray(new String[0])
                    );
                } else {
                    builder.setProjection(projectionEntityQuery.getProjection().get(0));
                }
            }
            if (projectionEntityQuery.getDistinctOn() != null) {
                if (projectionEntityQuery.getDistinctOn().size() > 1) {
                    builder.setDistinctOn(
                            projectionEntityQuery.getDistinctOn().get(0),
                            projectionEntityQuery.getDistinctOn().subList(1, projectionEntityQuery.getDistinctOn().size()).toArray(new String[0])
                    );
                } else {
                    builder.setProjection(projectionEntityQuery.getDistinctOn().get(0));
                }
            }
            return builder.build();
        } else {
            throw new UnsupportedOperationException();
        }

    }

    private com.google.cloud.datastore.StructuredQuery.Filter convertFilter(final StructuredQuery.Filter filter) {
        final com.google.cloud.datastore.StructuredQuery.Filter datastoreFilter;
        if (filter instanceof StructuredQuery.CompositeFilter) {
            final StructuredQuery.CompositeFilter compositeFilter = (StructuredQuery.CompositeFilter) filter;
            if (((StructuredQuery.CompositeFilter) filter).getFilters().size() > 1) {
                final com.google.cloud.datastore.StructuredQuery.Filter[] filters = compositeFilter.getFilters().subList(1, compositeFilter.getFilters().size())
                        .stream().map(
                                f -> convertFilter(f)
                        ).collect(Collectors.toList()).toArray(new com.google.cloud.datastore.StructuredQuery.Filter[0]);
                datastoreFilter = com.google.cloud.datastore.StructuredQuery.CompositeFilter.and(convertFilter(compositeFilter.getFilters().get(0)), filters);
            } else {
                datastoreFilter = com.google.cloud.datastore.StructuredQuery.CompositeFilter.and(convertFilter(compositeFilter.getFilters().get(0)));
            }

        } else if (filter instanceof StructuredQuery.PropertyFilter) {
            final StructuredQuery.PropertyFilter propertyFilter = (StructuredQuery.PropertyFilter) filter;
            final String name = propertyFilter.getProperty();
            switch (propertyFilter.getOperator()) {
                case EQUAL:
                    datastoreFilter = com.google.cloud.datastore.StructuredQuery.PropertyFilter.eq(name, DatastoreService.MARSHALLER.convertValue(propertyFilter.getValue()));
                    break;
                case LESS_THAN:
                    datastoreFilter = com.google.cloud.datastore.StructuredQuery.PropertyFilter.lt(name, DatastoreService.MARSHALLER.convertValue(propertyFilter.getValue()));
                    break;
                case LESS_THAN_OR_EQUAL:
                    datastoreFilter = com.google.cloud.datastore.StructuredQuery.PropertyFilter.le(name, DatastoreService.MARSHALLER.convertValue(propertyFilter.getValue()));
                    break;
                case GREATER_THAN:
                    datastoreFilter = com.google.cloud.datastore.StructuredQuery.PropertyFilter.gt(name, DatastoreService.MARSHALLER.convertValue(propertyFilter.getValue()));
                    break;
                case GREATER_THAN_OR_EQUAL:
                    datastoreFilter = com.google.cloud.datastore.StructuredQuery.PropertyFilter.ge(name, DatastoreService.MARSHALLER.convertValue(propertyFilter.getValue()));
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        } else {
            throw new UnsupportedOperationException();
        }
        return datastoreFilter;
    }
}
