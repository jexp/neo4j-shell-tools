package org.neo4j.shell.tools.imp;

import org.neo4j.cypher.export.CypherResultSubGraph;
import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.shell.*;
import org.neo4j.shell.impl.AbstractApp;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.shell.tools.imp.format.Format;
import org.neo4j.shell.tools.imp.format.XmlGraphMLFormat;
import org.neo4j.shell.tools.imp.util.Config;
import org.neo4j.shell.tools.imp.util.FileUtils;
import org.neo4j.shell.tools.imp.util.ProgressReporter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.Writer;

/**
 * TODO: arrays, labels, rel-types, key-types
 * @author mh
 * @since 17.01.14
 */
public class ExportGraphMLApp extends AbstractApp {

    {
        addOptionDefinition( "o", new OptionDefinition( OptionValueType.MUST,
                "Output GraphML file" ) );
        addOptionDefinition( "t", new OptionDefinition( OptionValueType.MAY,
                "Write key types upfront (double pass)" ) );
        addOptionDefinition( "r", new OptionDefinition( OptionValueType.MAY,
                "Add all nodes of selected relationships" ) );
    }

    @Override
    public String getName() {
        return "export-graphml";
    }


    @Override
    public GraphDatabaseShellServer getServer() {
        return (GraphDatabaseShellServer) super.getServer();
    }

    private SubGraph cypherResultSubGraph(String query, boolean relsBetween) {
        GraphDatabaseAPI db = getServer().getDb();
        try (Transaction tx = db.beginTx()) {
            Result result = db.execute(query);
            SubGraph subGraph = CypherResultSubGraph.from(result, db, relsBetween);
            tx.success();
            return subGraph;
        }
    }


    @Override
    public Continuation execute(AppCommandParser parser, Session session, Output out) throws Exception {
        Config config = Config.fromOptions(parser);

        String fileName = parser.option("o", null);
        boolean relsBetween = parser.options().containsKey("r");
        Writer writer = FileUtils.getPrintWriter(fileName,out);

        ProgressReporter reporter = new ProgressReporter(null, out);

        GraphDatabaseService db = getServer().getDb();

        Format exportFormat = new XmlGraphMLFormat(db);
        String query = Config.extractQuery(parser);
        SubGraph graph = query.isEmpty() ? new DatabaseSubGraph(db) : cypherResultSubGraph(query,relsBetween);
        exportFormat.dump(graph, writer, reporter, config);
        writer.close();
        reporter.progress("Wrote to GraphML-file " + fileName);
        return Continuation.INPUT_COMPLETE;
    }
}
