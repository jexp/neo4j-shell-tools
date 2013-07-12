package org.neo4j.shell.tools.imp;

import org.neo4j.geoff.Geoff;
import org.neo4j.geoff.except.GeoffLoadException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.shell.*;
import org.neo4j.shell.kernel.apps.GraphDatabaseApp;
import org.neo4j.shell.tools.imp.util.CountingReader;
import org.neo4j.shell.tools.imp.util.FileUtils;

import java.io.*;
import java.util.Collections;
import java.util.Map;

/**
 * Created by mh on 04.07.13.
 */
public class ImportGeoffApp extends GraphDatabaseApp {

    {
        addOptionDefinition( "g", new OptionDefinition( OptionValueType.MUST,
                "Input Geoff file" ) );
    }

    @Override
    public String getName() {
        return "import-geoff";
    }

    @Override
    protected Continuation exec(AppCommandParser parser, Session session, Output out) throws Exception {
        String fileName = parser.option("g", null);
        CountingReader reader = FileUtils.readerFor(fileName);

        if (reader!=null) {
            int count = execute(reader);
            out.println("Geoff import of "+fileName+" created "+count+" entities.");
        }
        if (reader!=null) reader.close();
        return Continuation.INPUT_COMPLETE;
    }

    private int execute(Reader reader) throws GeoffLoadException, IOException {
        Map<String, PropertyContainer> result = Geoff.loadIntoNeo4j(reader, getServer().getDb(), Collections.<String, PropertyContainer>emptyMap());
        return result.size();
    }
}
