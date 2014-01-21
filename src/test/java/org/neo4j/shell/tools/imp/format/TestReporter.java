package org.neo4j.shell.tools.imp.format;

import org.neo4j.shell.tools.imp.util.ElementCounter;
import org.neo4j.shell.tools.imp.util.Reporter;

/**
 * @author mh
 * @since 19.01.14
 */
public class TestReporter implements Reporter {
    ElementCounter counter = new ElementCounter();
    @Override
    public void progress(String msg) {
    }

    @Override
    public void update(long nodes, long rels, long properties) {
        counter.update(nodes,rels,properties);
    }

    @Override
    public ElementCounter getTotal() {
        return counter;
    }
}
