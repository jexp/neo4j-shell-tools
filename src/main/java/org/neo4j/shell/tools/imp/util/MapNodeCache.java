package org.neo4j.shell.tools.imp.util;

import org.mapdb.DBMaker;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Created by mh on 10.07.13.
 */
public class MapNodeCache implements NodeCache {
    private final Map<String,Long> map;

    public MapNodeCache(Map<String, Long> map) {
        this.map = map;
    }

    public static NodeCache usingMapDb() {
        return new MapNodeCache(DBMaker.<String,Long>newTempHashMap());
    }
    public static NodeCache usingHashMap() {
        return new MapNodeCache(new HashMap<String, Long>(1024*1024,0.95f));
    }

    @Override
    public void put(String name, long id) {
        map.put(name,id);
    }

    @Override
    public long get(String name) {
        Long id = map.get(name);
        if (id==null) throw new NoSuchElementException("No Element for "+name);
        return id;
    }
}
