package org.neo4j.shell.tools.imp;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.shell.*;
import org.neo4j.shell.impl.AbstractApp;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.shell.tools.imp.util.*;

import java.io.*;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author mh
 * @since 04.07.13
 */
public class ImportCypherApp extends AbstractApp {


    {
        addOptionDefinition( "i", new OptionDefinition( OptionValueType.MUST,
                "Input CSV/TSV file" ) );
        addOptionDefinition( "o", new OptionDefinition( OptionValueType.MUST,
                "Output CSV/TSV file" ) );
        addOptionDefinition("b", new OptionDefinition(OptionValueType.MUST,
                "Batch Size default " + Config.DEFAULT_BATCH_SIZE));
        addOptionDefinition( "d", new OptionDefinition( OptionValueType.MUST,
                "Delimeter, default is comma ',' " ) );
        addOptionDefinition( "q", new OptionDefinition( OptionValueType.NONE,
                "Quoted Strings in file" ) );
        addOptionDefinition( "s", new OptionDefinition( OptionValueType.NONE,
                "Silent Operations" ) );
    }

    protected GraphDatabaseService getEngine() {
        return getServer().getDb();
    }

    @Override
    public String getName() {
        return "import-cypher";
    }


    @Override
    public GraphDatabaseShellServer getServer() {
        return (GraphDatabaseShellServer) super.getServer();
    }

    @Override
    public Continuation execute(AppCommandParser parser, Session session, Output out) throws Exception {
        Config config = Config.fromOptions(parser);
        char delim = delim(parser.option("d", ","));
        int batchSize = Integer.parseInt(parser.option("b", String.valueOf(Config.DEFAULT_BATCH_SIZE)));
        boolean quotes = parser.options().containsKey("q");
        String inputFileName = parser.option("i", null);
        boolean silent = parser.options().containsKey("s");
        CountingReader inputFile = FileUtils.readerFor(inputFileName);
        String fileName = parser.option("o", null);
        Writer outputFile = FileUtils.getPrintWriter(fileName, out);

        String query = Config.extractQuery(parser);

        if (!silent) {
            out.println(String.format("Query: %s infile %s delim '%s' quoted %s outfile %s batch-size %d",
                    query, name(inputFileName), delim, quotes, name(fileName), batchSize));
        }

        CSVReader reader = createReader(inputFile, config);

        CSVWriter writer = createWriter(outputFile, config);

        int count;
        if (reader==null) {
            count = execute(query, writer);
        } else {
            count = executeOnInput(reader, query, writer, config, new ProgressReporter(inputFile,out));
        }
        if (!silent) {
            out.println("Import statement execution created " + count + " rows of output.");
        }
        if (reader!=null) reader.close();
        if (writer!=null) writer.close();
        return Continuation.INPUT_COMPLETE;
    }

    private String name(Object file) {
        if (file==null) return "(none)";
        return file.toString();
    }

    private char delim(String value) {
        if (value.length()==1) return value.charAt(0);
        if (value.contains("\\t")) return '\t';
        if (value.contains(" ")) return ' ';
        throw new RuntimeException("Illegal delimiter '"+value+"'");
    }

    private CSVWriter createWriter(Writer file, Config config) throws IOException {
        if (file==null) return null;
        return config.isQuotes() ? new CSVWriter(file,config.getDelimChar(), Config.QUOTECHAR) : new CSVWriter(file,config.getDelimChar());
    }

    private CSVReader createReader(CountingReader reader, Config config) throws FileNotFoundException {
        if (reader==null) return null;
        return config.isQuotes() ? new CSVReader(reader,config.getDelimChar(), Config.QUOTECHAR) : new CSVReader(reader,config.getDelimChar());
    }

    private int execute(String query, CSVWriter writer) {
        Result result = getEngine().execute(query);
        return writeResult(result, writer,true);
    }


    private int executeOnInput(CSVReader reader, String query, CSVWriter writer, Config config, ProgressReporter reporter) throws IOException {
        Map<String, Object> params = createParams(reader);
        Map<String, Type> types = extractTypes(params);
        Map<String, String> replacements = computeReplacements(params, query);
        String[] input;
        boolean first = true;
        int outCount = 0;
        try (BatchTransaction tx = new BatchTransaction(getServer().getDb(),config.getBatchSize(),reporter)) {
            while ((input = reader.readNext()) != null) {
                Map<String, Object> queryParams = update(params, types, input);
                String newQuery = applyReplacements(query, replacements, queryParams);
                Result result = getEngine().execute(newQuery, queryParams);
                outCount += writeResult(result, writer, first);
                first = false;
                ProgressReporter.update(result.getQueryStatistics(), reporter);
                tx.increment();
            }
        }
        return outCount;
    }

    private Map<String, Type> extractTypes(Map<String, Object> params) {
        Map<String,Object> newParams = new LinkedHashMap<>();
        Map<String,Type> types = new LinkedHashMap<>();
        for (String header : params.keySet()) {
            if (header.contains(":")) {
                String[] parts = header.split(":");
                newParams.put(parts[0],null);
                types.put(parts[0],Type.fromString(parts[1]));
            } else {
                newParams.put(header,null);
                types.put(header,Type.STRING);
            }
        }
        params.clear();
        params.putAll(newParams);
        return types;
    }

    private String applyReplacements(String query, Map<String, String> replacements, Map<String, Object> queryParams) {
        for (Map.Entry<String, String> entry : replacements.entrySet()) {
            Object value = queryParams.get(entry.getKey());
            query = query.replace(entry.getValue(), String.valueOf(value));
        }
        return query;
    }

    private Map<String, String> computeReplacements(Map<String, Object> params, String query) {
        Map<String, String> result = new HashMap<>();
        for (String name : params.keySet()) {
            String pattern = "#{" + name + "}";
            if (query.contains(pattern)) result.put(name,pattern);
        }
        return result;
    }

    private int writeResult(Result result, CSVWriter writer, boolean first) {
        if (writer==null) return IteratorUtil.count(result);
        String[] cols = new String[result.columns().size()];
        result.columns().toArray(cols);
        String[] data = new String[cols.length];
        if (first) {
            writer.writeNext(cols);
        }

        int count=0;
        while (result.hasNext()) {
            Map<String, Object> row= result.next();
            writeRow(writer, cols, data, row);
            count++;
        }
        return count;
    }

    private void writeRow(CSVWriter writer, String[] cols, String[] data, Map<String, Object> row) {
        for (int i = 0; i < cols.length; i++) {
            String col = cols[i];
            data[i]= toString(row, col);
        }
        writer.writeNext(data);
    }

    private String toString(Map<String, Object> row, String col) {
        Object value = row.get(col);
        if (value instanceof Number) {
            return FormatUtils.formatNumber((Number) value);
        }
        return value == null ? null : value.toString();
    }

    private Map<String, Object> createParams(CSVReader reader) throws IOException {
        String[] header = reader.readNext();
        Map<String,Object> params=new LinkedHashMap<>();
        for (String name : header) {
            params.put(name,null);
        }
        return params;
    }

    private Map<String, Object> update(Map<String, Object> params, Map<String, Type> types, String[] input) {
        int col=0;
        for (Map.Entry<String, Object> param : params.entrySet()) {
            Type type = types.get(param.getKey());
            Object value = type.convert(input[col++]);
            param.setValue(value);
        }
        return params;
    }
}
