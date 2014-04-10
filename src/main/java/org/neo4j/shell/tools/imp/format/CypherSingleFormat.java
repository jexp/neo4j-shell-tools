package org.neo4j.shell.tools.imp.format;

import org.neo4j.cypher.export.SubGraph;
import org.neo4j.cypher.export.SubGraphExporter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.shell.tools.imp.util.Config;
import org.neo4j.shell.tools.imp.util.ElementCounter;
import org.neo4j.shell.tools.imp.util.Reporter;

import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;

/**
 * @author mh
 * @since 17.01.14
 */
public class CypherSingleFormat implements Format {
    private final GraphDatabaseService db;

    public CypherSingleFormat(GraphDatabaseService db) {
        this.db = db;
    }

    @Override
    public ElementCounter load(Reader reader, Reporter reporter, Config config) {
        throw new UnsupportedOperationException("Import Cypher directly via the shell"); // todo auto-params
    }

    @Override
    public ElementCounter dump(SubGraph graph, Writer writer, Reporter reporter, Config config) {
        try (Transaction tx = db.beginTx()) {
            PrintWriter out = new PrintWriter(writer);
            new SubGraphExporter(graph).export(out);
            tx.success();
            return reporter.getTotal();
        }
    }
}
