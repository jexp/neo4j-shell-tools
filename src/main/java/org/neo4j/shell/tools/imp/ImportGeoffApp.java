package org.neo4j.shell.tools.imp;

import com.nigelsmall.geoff.Subgraph;
import com.nigelsmall.geoff.loader.NeoLoader;
import com.nigelsmall.geoff.reader.GeoffReader;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.shell.*;
import org.neo4j.shell.impl.AbstractApp;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.shell.tools.imp.util.CountingReader;
import org.neo4j.shell.tools.imp.util.FileUtils;

import java.io.IOException;
import java.io.Reader;
import java.util.Map;

/**
 * Created by mh on 04.07.13.
 */
public class ImportGeoffApp extends AbstractApp {

    {
        addOptionDefinition( "g", new OptionDefinition( OptionValueType.MUST,
                "Input Geoff file" ) );
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
        String fileName = parser.option("g", null);
        CountingReader reader = FileUtils.readerFor(fileName);

        if (reader!=null) {
            int count = execute(reader);
            out.println("Geoff import of "+fileName+" created "+count+" entities.");
        }
        if (reader!=null) reader.close();
        return Continuation.INPUT_COMPLETE;
    }

    private int execute(Reader reader) throws   IOException {
        GraphDatabaseService db = getServer().getDb();
        try (Transaction tx = db.beginTx())  {
            NeoLoader loader = new NeoLoader(db);
            Subgraph subgraph = new GeoffReader(reader).readSubgraph();
            Map<String, Node> result = loader.load(subgraph);
            tx.success();
            return result.size();
        }
    }
}
