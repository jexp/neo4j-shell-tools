package org.neo4j.shell.tools.imp;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.geoff.Geoff;
import org.neo4j.geoff.GeoffLoader;
import org.neo4j.geoff.except.GeoffLoadException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.shell.*;
import org.neo4j.shell.kernel.apps.GraphDatabaseApp;

import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
        File geoffFile = fileFor(parser, "g");
        BufferedReader geoffReader = createReader(geoffFile);

        int count = 0;
        if (geoffReader!=null) {
            count = execute(geoffReader);
        }
        out.println("Geoff statement execution created "+count+" entities.");
        if (geoffReader!=null) geoffReader.close();
        return Continuation.INPUT_COMPLETE;
    }

    protected File fileFor(AppCommandParser parser, String option) {
        String fileName = parser.option(option, null);
        if (fileName==null) return null;
        File file = new File(fileName);
        if (file.exists() && file.canRead() && file.isFile()) return file;
        return null;
    }

    protected BufferedReader createReader(File inputFile) throws FileNotFoundException {
        if (inputFile==null) return null;
        FileReader reader = new FileReader(inputFile);
        return new BufferedReader(reader);
    }

    private int execute(Reader reader) throws GeoffLoadException, IOException {
        Map<String, PropertyContainer> result = Geoff.loadIntoNeo4j(reader, getServer().getDb(), Collections.<String, PropertyContainer>emptyMap());
        return result.size();
    }
}
