package org.neo4j.shell.tools.imp.util;

import org.mapdb.DBMaker;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Created by mh on 10.07.13.
 */
public class MapNodeCache<K, V> implements NodeCache<K, V> {
    private final Map<K, V> map;

    public MapNodeCache(Map<K, V> map) {
        this.map = map;
    }

    public static <K, V>  NodeCache<K, V> usingMapDb() {
        return new MapNodeCache(DBMaker.<K, V>newTempHashMap());
    }
    public static <K, V>  NodeCache<K, V> usingHashMap() {
        return new MapNodeCache(new HashMap<K, V>(1024*1024,0.95f));
    }

    @Override
    public void put(K name, V id) {
        map.put(name, id);
    }

    @Override
    public V get(K name) {
        V id = map.get(name);
        if (id==null) throw new NoSuchElementException("No Element for "+name);
        return id;
    }
}