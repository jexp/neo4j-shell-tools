package org.neo4j.shell.tools.imp.format;

import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.*;
import org.neo4j.shell.tools.imp.format.graphml.SimpleGraphMLWriter;
import org.neo4j.shell.tools.imp.util.Config;
import org.neo4j.shell.tools.imp.util.ElementCounter;
import org.neo4j.shell.tools.imp.util.Reporter;

import java.io.Reader;
import java.io.Writer;

import static org.neo4j.helpers.collection.Iterables.join;

/**
 * @author mh
 * @since 21.01.14
 */
public class SimpleGraphMLFormat implements Format {
    private final GraphDatabaseService db;

    public SimpleGraphMLFormat(GraphDatabaseService db) {
        this.db = db;
    }

    @Override
    public ElementCounter load(Reader reader, Reporter reporter, Config config) throws Exception {
        return null;
    }

    @Override
    public ElementCounter dump(SubGraph graph, Writer writer, Reporter reporter, Config config) throws Exception {
        try (Transaction tx = db.beginTx()) {
            SimpleGraphMLWriter graphMlWriter = new SimpleGraphMLWriter(db);
            graphMlWriter.write(graph, writer, reporter, config);
            tx.success();
        }
        return reporter.getTotal();
    }
}


