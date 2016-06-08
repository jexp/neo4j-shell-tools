package org.neo4j.shell.tools.imp;

import org.neo4j.cypher.export.CypherResultSubGraph;
import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.shell.*;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.shell.tools.imp.format.kryo.KryoWriter;
import org.neo4j.shell.tools.imp.util.Config;
import org.neo4j.shell.tools.imp.util.ProgressReporter;

import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.DeflaterOutputStream;

/**
 * Created by efulton on 5/5/16.
 */
public class ExportBinaryApp extends BinaryApp {

    {
        addOptionDefinition( "o", new OptionDefinition( OptionValueType.MUST, "Output Binary file" ) );
        addOptionDefinition( "f", new OptionDefinition( OptionValueType.MAY, "Binary format (kryo is the default/only option right now)" ) );
    }

    @Override
    public String getName() {
        return "export-binary";
    }

    @Override
    public GraphDatabaseShellServer getServer() {
        return (GraphDatabaseShellServer) super.getServer();
    }

    @Override
    public Continuation execute(AppCommandParser parser, Session session, Output out) throws Exception {
        Config config = Config.fromOptions(parser);
        String fileName = parser.option("o", null);
        String format = parser.option("f", "kryo");
        ProgressReporter reporter = new ProgressReporter(null, out);
        GraphDatabaseService db = getServer().getDb();
        SubGraph graph = new DatabaseSubGraph(db);

        if (fileName == null) {
            throw new RuntimeException("Output file option is required");
        }

        switch (format) {
            case KRYO:
                writeOutputType(KRYO, fileName);
                writeKryo(fileName, db, graph, reporter, config);
                break;
            case CBOR:
                throw new RuntimeException(String.format("%s is not supported yet", CBOR));
            case PACKSTREAM:
                throw new RuntimeException(String.format("%s is not supported yet", PACKSTREAM));
        }

        reporter.progress("Wrote to binary file " + fileName);
        return Continuation.INPUT_COMPLETE;
    }

    private void writeOutputType(String type, String fileName) throws IOException {
        try (FileWriter fileWriter = new FileWriter(fileName)) {
            fileWriter.write(type);
            fileWriter.flush();
            fileWriter.close();
        }
    }

    private void writeKryo(String fileName, GraphDatabaseService db, SubGraph graph, ProgressReporter reporter, Config config) throws Exception {
        OutputStream outputStream = new DeflaterOutputStream(new FileOutputStream(fileName, true));
        com.esotericsoftware.kryo.io.Output output = new com.esotericsoftware.kryo.io.Output(outputStream);
        try (Transaction tx = db.beginTx()) {
            KryoWriter kryoWriter = new KryoWriter();
            kryoWriter.write(graph, output, reporter, config);
            tx.success();
        } finally {
            output.close();
        }
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
}