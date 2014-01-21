package org.neo4j.shell.tools.imp.util;

/**
* Created by mh on 10.07.13.
*/
public interface Reporter {
    void progress(String msg);
    void update(long nodes, long rels, long properties);

    ElementCounter getTotal();
}
