package com.cloudaware.store;

import com.cloudaware.store.model.Entity;
import com.cloudaware.store.model.FullEntity;
import com.cloudaware.store.model.IncompleteKey;
import com.cloudaware.store.model.Key;
import com.cloudaware.store.model.ProjectionEntity;
import com.cloudaware.store.model.Value;

public interface Unmarshaller<E, FE, PE, K, IK, V> {

    Key convertKey(K key);

    IncompleteKey convertIncompleteKey(IK key);

    Entity convertEntity(E entity);

    FullEntity convertFullEntity(FE entity);

    ProjectionEntity convertProjectionEntity(PE entity);

    Value<?> convertValue(V value);

}
