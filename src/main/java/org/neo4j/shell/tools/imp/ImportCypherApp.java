package org.neo4j.shell.tools.imp;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.shell.*;
import org.neo4j.shell.kernel.apps.GraphDatabaseApp;

import java.io.*;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by mh on 04.07.13.
 */
public class ImportCypherApp extends GraphDatabaseApp {

    public static final char QUOTECHAR = '"';
    public static final int DEFAULT_BATCH_SIZE = 20000;

    private ExecutionEngine engine;

    {
        addOptionDefinition( "i", new OptionDefinition( OptionValueType.MUST,
                "Input CSV/TSV file" ) );
        addOptionDefinition( "o", new OptionDefinition( OptionValueType.MUST,
                "Output CSV/TSV file" ) );
        addOptionDefinition( "b", new OptionDefinition( OptionValueType.MUST,
                "Batch Size default "+DEFAULT_BATCH_SIZE ) );
        addOptionDefinition( "d", new OptionDefinition( OptionValueType.MUST,
                "Delimeter, default is comma ',' " ) );
        addOptionDefinition( "q", new OptionDefinition( OptionValueType.NONE,
                "Quoted Strings in file" ) );
    }

    protected ExecutionEngine getEngine() {
        if (engine==null) engine=new ExecutionEngine(getServer().getDb(), StringLogger.SYSTEM);
        return engine;
    }

    @Override
    public String getName() {
        return "import-cypher";
    }


    @Override
    protected Continuation exec(AppCommandParser parser, Session session, Output out) throws Exception {
        char delim = delim(parser.option("d", ","));
        int batchSize = Integer.parseInt(parser.option("b", String.valueOf(DEFAULT_BATCH_SIZE)));
        boolean quotes = parser.options().containsKey("q");
        File inputFile = fileFor(parser, "i");
        File outputFile = fileFor(parser, "o");
        String query = extractQuery(parser);

        out.println(String.format("Query: %s infile %s delim '%s' quoted %s outfile %s batch-size %d",
                                   query,name(inputFile),delim,quotes,name(outputFile),batchSize));

        CSVReader reader = createReader(inputFile, delim, quotes);

        CSVWriter writer = createWriter(outputFile, delim, quotes);

        int count;
        if (reader==null) {
            count = execute(query, writer);
        } else {
            count = executeOnInput(reader, query, writer, batchSize);
        }
        out.println("Import statement execution created "+count+" rows of output.");
        if (reader!=null) reader.close();
        if (writer!=null) writer.close();
        return Continuation.INPUT_COMPLETE;
    }

    private String name(File file) {
        if (file==null) return "(none)";
        return file.getName();
    }

    private char delim(String value) {
        if (value.length()==1) return value.charAt(0);
        if (value.contains("\\t")) return '\t';
        if (value.contains(" ")) return ' ';
        throw new RuntimeException("Illegal delimiter '"+value+"'");
    }

    private String extractQuery(AppCommandParser parser) {
        String line = parser.getLineWithoutApp().trim();
        Map<String, String> options = new HashMap<String, String>(parser.options());
        while (!options.isEmpty() && line.startsWith("-")) {
            String option = options.remove(line.substring(1, 2));
            int offset = 3 + option.length();
            if (option.trim().isEmpty()) offset+=2; // for quoted space or tab
            int idx = line.indexOf(" ", offset);
            if (idx != -1) line = line.substring(idx+1).trim();
        }
        return line;
    }

    private CSVWriter createWriter(File outputFile, char delim, boolean quotes) throws IOException {
        if (outputFile==null) return null;
        FileWriter file = new FileWriter(outputFile);
        return quotes ? new CSVWriter(file,delim, QUOTECHAR) : new CSVWriter(file,delim);
    }

    private CSVReader createReader(File inputFile, char delim, boolean quotes) throws FileNotFoundException {
        if (inputFile==null) return null;
        FileReader reader = new FileReader(inputFile);
        return quotes ? new CSVReader(reader,delim, QUOTECHAR) : new CSVReader(reader,delim);
    }

    private int execute(String query, CSVWriter writer) {
        ExecutionResult result = getEngine().execute(query);
        return writeResult(result, writer,true);
    }

    private int executeOnInput(CSVReader reader, String query, CSVWriter writer, int batchSize) throws IOException {
        Map<String, Object> params = createParams(reader);
        String[] input;
        boolean first = true;
        int outCount = 0, inCount = 0;
        Transaction tx = getServer().getDb().beginTx();
        try {
            while ((input = reader.readNext()) != null) {
                ExecutionResult result = getEngine().execute(query, update(params, input));
                outCount += writeResult(result, writer, first);
                first = false;
                inCount++;
                if (inCount % batchSize == 0) {
                    tx.success();
                    tx.finish();
                    tx = getServer().getDb().beginTx();
                }
            }
            tx.success();
        } finally {
            tx.finish();
        }
        return outCount;
    }

    private int writeResult(ExecutionResult result, CSVWriter writer, boolean first) {
        if (writer==null) return IteratorUtil.count(result);
        String[] cols = new String[result.columns().size()];
        result.columns().toArray(cols);
        String[] data = new String[cols.length];
        if (first) {
            writer.writeNext(cols);
        }

        int count=0;
        for (Map<String, Object> row : result) {
            writeRow(writer, cols, data, row);
            count++;
        }
        return count;
    }

    private void writeRow(CSVWriter writer, String[] cols, String[] data, Map<String, Object> row) {
        for (int i = 0; i < cols.length; i++) {
            String col = cols[i];
            data[i]=row.get(col).toString();
        }
        writer.writeNext(data);
    }

    private Map<String, Object> createParams(CSVReader reader) throws IOException {
        String[] header = reader.readNext();
        Map<String,Object> params=new LinkedHashMap<String,Object>();
        for (String name : header) {
            params.put(name,null);
        }
        return params;
    }

    private Map<String, Object> update(Map<String, Object> params, String[] input) {
        int col=0;
        for (Map.Entry<String, Object> param : params.entrySet()) {
            param.setValue(input[col++]);
        }
        return params;
    }

    private File fileFor(AppCommandParser parser, String option) {
        String fileName = parser.option(option, null);
        if (fileName==null) return null;
        File file = new File(fileName);
        if (option.equals("o") || file.exists() && file.canRead() && file.isFile()) return file;
        return null;
    }
}
