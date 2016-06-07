package org.neo4j.shell.tools.imp.util;

/**
* Created by mh on 10.07.13.
*/
public interface NodeCache<K, V> {
    void put(K name, V id);
    V get(K name);
}