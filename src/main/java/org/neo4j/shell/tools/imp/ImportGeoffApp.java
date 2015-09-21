package org.neo4j.shell.tools.imp;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.shell.*;
import org.neo4j.shell.impl.AbstractApp;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.shell.tools.imp.format.GeoffFormat;
import org.neo4j.shell.tools.imp.util.Config;
import org.neo4j.shell.tools.imp.util.CountingReader;
import org.neo4j.shell.tools.imp.util.FileUtils;
import org.neo4j.shell.tools.imp.util.ProgressReporter;

import java.io.IOException;

/**
 * @author mh
 * @since 04.07.13
 */
public class ImportGeoffApp extends AbstractApp {

    {
        addOptionDefinition( "i", new OptionDefinition( OptionValueType.MUST,
                "Input file" ) );
    }

    @Override
    public String getName() {
        return "import-geoff";
    }

    @Override
    public GraphDatabaseShellServer getServer() {
        return (GraphDatabaseShellServer) super.getServer();
    }

    @Override
    public Continuation execute(AppCommandParser parser, Session session, Output out) throws Exception {
        String fileName = parser.option("i", null);
        Config config = Config.fromOptions(parser);
        CountingReader reader = FileUtils.readerFor(fileName);
        if (reader!=null) {
            int count = execute(reader, new ProgressReporter(reader, out), config);
            out.println("Geoff import of "+fileName+" created "+count+" entities.");
        }
        if (reader!=null) reader.close();
        return Continuation.INPUT_COMPLETE;
    }

    private int execute(CountingReader reader, ProgressReporter reporter, Config config) throws   IOException {
        GraphDatabaseService db = getServer().getDb();
        return new GeoffFormat(db).load(reader, reporter, config).getNodes();
    }
}
