package org.neo4j.shell.tools.imp.util;

/**
* Created by mh on 10.07.13.
*/
public interface NodeCache {
    void put(String name, long id);
    long get(String name);
}
