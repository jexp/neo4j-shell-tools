package org.neo4j.shell.tools.imp;

import org.neo4j.cypher.export.CypherResultSubGraph;
import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.shell.*;
import org.neo4j.shell.impl.AbstractApp;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.shell.tools.imp.format.Format;
import org.neo4j.shell.tools.imp.format.XmlGraphMLFormat;
import org.neo4j.shell.tools.imp.util.Config;
import org.neo4j.shell.tools.imp.util.ProgressReporter;

import java.io.BufferedWriter;
import java.io.FileWriter;

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

	private ExecutionEngine engine;
	protected ExecutionEngine getEngine() {
		if (engine==null) engine=new ExecutionEngine(getServer().getDb(), StringLogger.SYSTEM);
		return engine;
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
		try (Transaction tx = getServer().getDb().beginTx()) {
			ExecutionResult result = getEngine().execute(query);
			SubGraph subGraph = CypherResultSubGraph.from(result, getServer().getDb(), relsBetween);
			tx.success();
			return subGraph;
		}
	}


	@Override
	public Continuation execute(AppCommandParser parser, Session session, Output out) throws Exception {
		Config config = Config.fromOptions(parser);

		String fileName = parser.option("o", null);
		boolean relsBetween = parser.options().containsKey("r");
		BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));

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
