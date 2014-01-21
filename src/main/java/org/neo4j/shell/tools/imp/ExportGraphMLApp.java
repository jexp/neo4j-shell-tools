package org.neo4j.shell.tools.imp;

import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.shell.*;
import org.neo4j.shell.impl.AbstractApp;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.shell.tools.imp.format.Config;
import org.neo4j.shell.tools.imp.format.Format;
import org.neo4j.shell.tools.imp.format.SimpleGraphMLFormat;
import org.neo4j.shell.tools.imp.format.XmlGraphMLFormat;
import org.neo4j.shell.tools.imp.format.graphml.XmlGraphMLWriter;
import org.neo4j.shell.tools.imp.util.ProgressReporter;
import org.neo4j.shell.tools.imp.util.Reporter;
import org.neo4j.tooling.GlobalGraphOperations;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import static java.util.Arrays.asList;
import static org.neo4j.helpers.collection.Iterables.join;

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
                "Write Key Types upfront (double pass)" ) );
    }

    @Override
    public String getName() {
        return "export-graphml";
    }


    @Override
    public GraphDatabaseShellServer getServer() {
        return (GraphDatabaseShellServer) super.getServer();
    }

    @Override
    public Continuation execute(AppCommandParser parser, Session session, Output out) throws Exception {
        String fileName = parser.option("o", null);
        BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
        ProgressReporter reporter = new ProgressReporter(null, out);
        GraphDatabaseService db = getServer().getDb();
        Format exportFormat = new XmlGraphMLFormat(db);
        exportFormat.dump(new DatabaseSubGraph(db), writer, reporter, Config.fromOptions(parser));
        writer.close();
        reporter.progress("Wrote to GraphML-file " + fileName);
        return Continuation.INPUT_COMPLETE;
    }
}
