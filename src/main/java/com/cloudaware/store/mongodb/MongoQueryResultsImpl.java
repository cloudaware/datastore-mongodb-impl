package com.cloudaware.store.mongodb;

import com.cloudaware.store.model.Query;
import com.cloudaware.store.model.QueryResults;
import com.cloudaware.store.model.StructuredQuery;
import com.cloudaware.store.model.ValueType;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonString;

import java.util.List;

import static com.cloudaware.store.mongodb.MongoStoreService.KEY_FIELD;

public class MongoQueryResultsImpl<T> extends AbstractIterator<T> implements QueryResults<T> {

    private final MongoCollection<BsonDocument> mongoCollection;
    private final Query<T> query;
    private MongoCursor<BsonDocument> cursor;

    public MongoQueryResultsImpl(final MongoStoreService mongoStoreService, final String projectId, final Query<T> query) {
        this.query = query;
        if (query instanceof StructuredQuery) {
            final StructuredQuery structuredQuery = (StructuredQuery) query;
            Preconditions.checkNotNull(structuredQuery.getKind());
            this.mongoCollection = mongoStoreService.getCollection(projectId, query.getNamespace(), structuredQuery.getKind());
        } else {
            this.mongoCollection = null;
            new UnsupportedOperationException();
        }
        makeRequest();
    }

    private void makeRequest() {
        if (query instanceof StructuredQuery) {
            final StructuredQuery structuredQuery = (StructuredQuery) query;
            final BsonDocument filter = convertFilter(structuredQuery.getFilter());

            if (structuredQuery.getDistinctOn() != null && structuredQuery.getDistinctOn().size() > 0) {
                final ImmutableList.Builder<BsonDocument> pipelineBuilder = ImmutableList.<BsonDocument>builder();
                //match - filter
                pipelineBuilder.add(new BsonDocument("$match", filter));
                //match - filter null in distinct values;
                final BsonDocument neNullMatch = new BsonDocument();
                for (final String distinct : (List<String>) structuredQuery.getDistinctOn()) {
                    neNullMatch.append(distinct, new BsonDocument("$ne", new BsonNull()));
                }
                pipelineBuilder.add(new BsonDocument("$match", neNullMatch));
                //sort - from sort if exist;
                final BsonDocument sort = new BsonDocument("_id", new BsonInt32(-1));
                if (structuredQuery.getOrderBy() != null && structuredQuery.getOrderBy().size() > 0) {
                    for (final StructuredQuery.OrderBy orderBy : (List<StructuredQuery.OrderBy>) structuredQuery.getOrderBy()) {
                        sort.append(orderBy.getProperty(), orderBy.getDirection().equals(StructuredQuery.OrderBy.Direction.ASCENDING) ? new BsonInt32(1) : new BsonInt32(-1));
                    }
                }
                pipelineBuilder.add(new BsonDocument("$sort", sort));
                //project - from fields if exist;
                final BsonDocument projection = new BsonDocument(KEY_FIELD, new BsonInt32(1));
                if (structuredQuery.getProjection() != null && structuredQuery.getProjection().size() > 0) {
                    for (final String projName : (List<String>) structuredQuery.getProjection()) {
                        projection.append(projName, new BsonInt32(1));
                    }
                    for (final String distinct : (List<String>) structuredQuery.getDistinctOn()) {
                        projection.append(distinct, new BsonInt32(1));
                    }
                }
                pipelineBuilder.add(new BsonDocument("$project", projection));
                //group - by distinct
                final BsonDocument group = new BsonDocument();
                for (final String distinct : (List<String>) structuredQuery.getDistinctOn()) {
                    group.append(distinct, new BsonString("$" + distinct));
                }
                final BsonDocument groupPipeline = new BsonDocument();
                groupPipeline.append("_id", group);
                groupPipeline.append("result", new BsonDocument("$first", new BsonString("$$ROOT")));
                pipelineBuilder.add(new BsonDocument("$group", groupPipeline));
                //project - from group response
                final BsonDocument resultProjection = new BsonDocument(KEY_FIELD, new BsonString("$result." + KEY_FIELD));
                if (structuredQuery.getProjection() != null && structuredQuery.getProjection().size() > 0) {
                    for (final String projName : (List<String>) structuredQuery.getProjection()) {
                        resultProjection.append(projName, new BsonString("$result." + projName));
                    }
                }
                for (final String distinct : (List<String>) structuredQuery.getDistinctOn()) {
                    resultProjection.append(distinct, new BsonString("$result." + distinct));
                }
                pipelineBuilder.add(new BsonDocument("$project", resultProjection));

                final AggregateIterable<BsonDocument> iterable = this.mongoCollection.aggregate(pipelineBuilder.build());
                this.cursor = iterable.iterator();
            } else {
                final FindIterable<BsonDocument> iterable = this.mongoCollection.find(
                        filter
                ).skip(structuredQuery.getOffset());
                if (structuredQuery.getLimit() != null) {
                    iterable.limit(structuredQuery.getLimit());
                }
                if (structuredQuery.getOrderBy() != null && structuredQuery.getOrderBy().size() > 0) {
                    final BsonDocument sort = new BsonDocument();
                    for (final StructuredQuery.OrderBy orderBy : (List<StructuredQuery.OrderBy>) structuredQuery.getOrderBy()) {
                        sort.append(orderBy.getProperty(), orderBy.getDirection().equals(StructuredQuery.OrderBy.Direction.ASCENDING) ? new BsonInt32(1) : new BsonInt32(-1));
                    }
                    iterable.sort(sort);
                }
                if (structuredQuery.getProjection() != null && structuredQuery.getProjection().size() > 0) {
                    final BsonDocument projection = new BsonDocument();
                    for (final String projName : (List<String>) structuredQuery.getProjection()) {
                        projection.append(projName, new BsonInt32(1));
                    }
                    iterable.projection(projection);
                }
                this.cursor = iterable.iterator();
            }
        } else {
            new UnsupportedOperationException();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected T computeNext() {
        if (this.cursor != null && !this.cursor.hasNext()) {
            this.cursor.close();
            return endOfData();
        }
        final T result;
        if (Query.ResultType.ENTITY.equals(query.getType())) {
            result = (T) MongoStoreService.UNMARSHALLER.convertEntity(cursor.next());
        } else if (Query.ResultType.KEY.equals(query.getType())) {
            result = (T) MongoStoreService.UNMARSHALLER.convertKey(cursor.next().getDocument(KEY_FIELD));
        } else if (Query.ResultType.PROJECTION_ENTITY.equals(query.getType())) {
            result = (T) MongoStoreService.UNMARSHALLER.convertProjectionEntity(cursor.next());
        } else {
            throw new UnsupportedOperationException();
        }

        return result;
    }

    private BsonDocument convertFilter(final StructuredQuery.Filter filter) {
        final BsonDocument bsonFilter = new BsonDocument();
        if (filter instanceof StructuredQuery.CompositeFilter) {
            final StructuredQuery.CompositeFilter compositeFilter = (StructuredQuery.CompositeFilter) filter;
            final String op;
            switch (compositeFilter.getOperator()) {
                case AND:
                    op = "$and";
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
            final BsonArray innerFilters = new BsonArray();
            for (final StructuredQuery.Filter f : compositeFilter.getFilters()) {
                innerFilters.add(convertFilter(f));
            }
            bsonFilter.append(op, innerFilters);
        } else if (filter instanceof StructuredQuery.PropertyFilter) {
            final StructuredQuery.PropertyFilter propertyFilter = (StructuredQuery.PropertyFilter) filter;
            final String name = propertyFilter.getProperty();
            if (ValueType.NULL.equals(propertyFilter.getValue().getType())) {
                if (propertyFilter.getOperator().equals(StructuredQuery.PropertyFilter.Operator.EQUAL)) {
                    bsonFilter.append(name, new BsonDocument("$eq", new BsonNull()));
                } else {
                    bsonFilter.append(name, new BsonDocument("$ne", new BsonNull()));
                }
            } else {
                final String op;
                switch (propertyFilter.getOperator()) {
                    case EQUAL:
                        op = "$eq";
                        break;
                    case LESS_THAN:
                        op = "$lt";
                        break;
                    case LESS_THAN_OR_EQUAL:
                        op = "$lte";
                        break;
                    case GREATER_THAN:
                        op = "$gt";
                        break;
                    case GREATER_THAN_OR_EQUAL:
                        op = "$gte";
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
                bsonFilter.append(name, new BsonDocument(op, MongoStoreService.MARSHALLER.convertValue(propertyFilter.getValue())));
            }
        }
        return bsonFilter;
    }
}
