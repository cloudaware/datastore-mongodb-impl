package com.cloudaware.store;

import com.cloudaware.store.model.Entity;
import com.cloudaware.store.model.Key;
import com.cloudaware.store.model.KeyFactory;
import com.cloudaware.store.model.Query;
import com.cloudaware.store.model.QueryResults;

import java.util.Iterator;
import java.util.List;

public interface StoreService {

    KeyFactory newKeyFactory();

    List<Entity> put(Iterable<Entity> entities, String... args);

    Entity put(Entity entity, String... args);

    Entity get(Key key, String... args);

    Iterator<Entity> get(Iterable<Key> keys, String... args);

    void delete(Iterable<Key> keys);

    <T> QueryResults<T> run(Query<T> query);
}
