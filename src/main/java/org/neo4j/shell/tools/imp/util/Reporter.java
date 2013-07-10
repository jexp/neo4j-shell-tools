package org.neo4j.shell.tools.imp.util;

/**
* Created by mh on 10.07.13.
*/
public interface Reporter {
    void progress(long nodes, long relationships, long properties);
    void finish(long nodes, long relationships, long properties);
}
