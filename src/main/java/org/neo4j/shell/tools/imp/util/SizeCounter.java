package org.neo4j.shell.tools.imp.util;

/**
* Created by mh on 10.07.13.
*/
interface SizeCounter {
    long getNewLines();
    long getCount();
    long getTotal();
    long getPercent();
}
