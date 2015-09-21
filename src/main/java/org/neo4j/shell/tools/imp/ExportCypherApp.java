package org.neo4j.shell.tools.imp;

import org.neo4j.cypher.export.CypherResultSubGraph;
import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.shell.*;
import org.neo4j.shell.impl.AbstractApp;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.shell.tools.imp.format.*;
import org.neo4j.shell.tools.imp.util.Config;
import org.neo4j.shell.tools.imp.util.ProgressReporter;

import java.io.*;

public class ExportCypherApp extends AbstractApp {

    {
        addOptionDefinition( "o", new OptionDefinition( OptionValueType.MUST,
                "Output Cypher file" ) );
        addOptionDefinition("b", new OptionDefinition(OptionValueType.MUST,
                "Batch Size default " + Config.DEFAULT_BATCH_SIZE));
        addOptionDefinition( "r", new OptionDefinition( OptionValueType.MAY,
                "Add all nodes of selected relationships" ) );
    }

    @Override
    public String getName() {
        return "export-cypher";
    }


    @Override
    public GraphDatabaseShellServer getServer() {
        return (GraphDatabaseShellServer) super.getServer();
    }

    private SubGraph cypherResultSubGraph(String query, boolean relsBetween) {
        GraphDatabaseService db = getServer().getDb();
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

        boolean relsBetween = parser.options().containsKey("r");


        ProgressReporter reporter = new ProgressReporter(null, out);

        GraphDatabaseService db = getServer().getDb();

        String query = Config.extractQuery(parser);
        SubGraph graph = query.isEmpty() ? new DatabaseSubGraph(db) : cypherResultSubGraph(query,relsBetween);

        String fileName = parser.option("o", null);
        try (Transaction tx = db.beginTx();PrintWriter printWriter = getPrintWriter(fileName, out)) {
            MultiStatementCypherSubGraphExporter exporter = new MultiStatementCypherSubGraphExporter(graph);
            exporter.export(printWriter, config.getBatchSize(), reporter);
            tx.success();
        }
        reporter.progress("Wrote to Cypher-file " + fileName);
        return Continuation.INPUT_COMPLETE;
    }

    private PrintWriter getPrintWriter(String fileName, Output out) throws IOException {
        Writer writer = (fileName == null) ? new OutputAsWriter(out) : new BufferedWriter(new FileWriter(fileName));
        return new PrintWriter(writer);
    }
}
