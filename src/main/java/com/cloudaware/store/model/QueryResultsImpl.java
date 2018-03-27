package com.cloudaware.store.model;

import com.google.common.collect.AbstractIterator;

public class QueryResultsImpl<T> extends AbstractIterator<T> implements QueryResults<T> {

    @Override
    protected T computeNext() {
        return null;
    }
}
