package com.cloudaware.store;

import com.cloudaware.store.model.Entity;
import com.cloudaware.store.model.FullEntity;
import com.cloudaware.store.model.IncompleteKey;
import com.cloudaware.store.model.Key;
import com.cloudaware.store.model.Value;

public interface Marshaller<E, FE, K, IK, V> {
    K convertKey(Key key);

    IK convertIncompleteKey(IncompleteKey key);

    E convertEntity(Entity entity);

    FE convertFullEntity(FullEntity entity);

    V convertValue(Value<?> value);

//    Key backConvertKey(K key);
//    IncompleteKey backConvertKey(IK key);
//    Entity backConvertEntity(E entity);
//    FullEntity backConvertEntity(FE entity);
//    Value<?>backConvertValue(V value);

}
