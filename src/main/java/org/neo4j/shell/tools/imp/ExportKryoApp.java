package org.neo4j.shell.tools.imp;

import org.neo4j.cypher.export.CypherResultSubGraph;
import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.shell.*;
import org.neo4j.shell.impl.AbstractApp;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.shell.tools.imp.format.kryo.KryoWriter;
import org.neo4j.shell.tools.imp.util.Config;
import org.neo4j.shell.tools.imp.util.ProgressReporter;

import java.io.FileOutputStream;

/**
 * Created by efulton on 5/5/16.
 */
public class ExportKryoApp extends AbstractApp {

  {
    addOptionDefinition( "o", new OptionDefinition( OptionValueType.MUST, "Output Binary file" ) );
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

    com.esotericsoftware.kryo.io.Output output =
        new com.esotericsoftware.kryo.io.Output(new FileOutputStream(fileName));

    ProgressReporter reporter = new ProgressReporter(null, out);

    GraphDatabaseService db = getServer().getDb();
    SubGraph graph = new DatabaseSubGraph(db);

    try (Transaction tx = db.beginTx()) {
      KryoWriter kryoWriter = new KryoWriter();
      kryoWriter.write(graph, output, reporter, config);
      tx.success();
    } finally {
      output.close();
    }

    reporter.progress("Wrote to binary file " + fileName);
    return Continuation.INPUT_COMPLETE;
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