package org.neo4j.shell.tools.imp.format;

import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.shell.tools.imp.format.graphml.XmlGraphMLWriter;
import org.neo4j.shell.tools.imp.util.Config;
import org.neo4j.shell.tools.imp.util.ElementCounter;
import org.neo4j.shell.tools.imp.util.Reporter;

import java.io.Reader;
import java.io.Writer;

/**
 * @author mh
 * @since 21.01.14
 */
public class XmlGraphMLFormat implements Format {
    private final GraphDatabaseService db;

    public XmlGraphMLFormat(GraphDatabaseService db) {
        this.db = db;
    }

    @Override
    public ElementCounter load(Reader reader, Reporter reporter, Config config) throws Exception {
        return null;
    }

    @Override
    public ElementCounter dump(SubGraph graph, Writer writer, Reporter reporter, Config config) throws Exception {
        try (Transaction tx = db.beginTx()) {
            XmlGraphMLWriter graphMlWriter = new XmlGraphMLWriter();
            graphMlWriter.write(graph, writer, reporter, config);
            tx.success();
        }
        return reporter.getTotal();
    }
}


