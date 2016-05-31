package org.neo4j.shell.tools.imp;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.shell.*;
import org.neo4j.shell.impl.AbstractApp;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.shell.tools.imp.util.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author mh
 * @since 04.07.13
 */
public class ImportCsvApp extends AbstractApp {


    {
        addOptionDefinition( "i", new OptionDefinition( OptionValueType.MUST,
                "Input CSV/TSV file" ) );
        addOptionDefinition( "o", new OptionDefinition( OptionValueType.MUST,
                "Output CSV/TSV file" ) );
        addOptionDefinition("b", new OptionDefinition(OptionValueType.MUST,
                "Batch Size default " + Config.DEFAULT_BATCH_SIZE));
        addOptionDefinition( "d", new OptionDefinition( OptionValueType.MUST,
                "Delimeter, default is comma ',' " ) );
        addOptionDefinition( "l", new OptionDefinition( OptionValueType.MAY,
                "Label to export" ) );
        addOptionDefinition( "q", new OptionDefinition( OptionValueType.NONE,
                "Quoted Strings in file" ) );
        addOptionDefinition( "s", new OptionDefinition( OptionValueType.NONE,
                "Silent Operations" ) );

    }

    @Override
    public String getName() {
        return "import-csv";
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
        boolean silent = parser.options().containsKey("s");
        String inputFileName = parser.option("i", null);
        CountingReader inputFile = FileUtils.readerFor(inputFileName);
        String fileName = parser.option("o", null);
        Writer outputFile = FileUtils.getPrintWriter(fileName, out);

        if (!silent) {
            out.println(String.format("Infile %s delim '%s' quoted %s outfile %s batch-size %d",
                    name(inputFileName),delim,quotes,name(fileName),batchSize));
        }

        CSVReader reader = createReader(inputFile, config);

        CSVWriter writer = createWriter(outputFile, config);

        int count;
        if (reader==null) {
            count = execute(writer);
        } else {
            count = executeOnInput(reader, writer, config, new ProgressReporter(inputFile,out));
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

    private int execute(CSVWriter writer) {
        return writeResult(writer,true, getServer().getDb());
    }


    private int executeOnInput(CSVReader reader, CSVWriter writer, Config config, ProgressReporter reporter) throws IOException {
        Map<String, Object> params = createParams(reader);
        Map<String, Type> types = extractTypes(params);
        String[] input;
        boolean first = true;
        int outCount = 0;
        try (BatchTransaction tx = new BatchTransaction(getServer().getDb(),config.getBatchSize(),reporter)) {
            while ((input = reader.readNext()) != null) {
                Map<String, Object> queryParams = update(params, types, input);
//                String newQuery = applyReplacements(query, replacements, queryParams);
//                SubGraph result = getEngine().execute(newQuery, queryParams);
//                outCount += writeResult(writer, first, getServer().getDb());
//                first = false;
//                ProgressReporter.update(result.getQueryStatistics(), reporter);
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

    private int writeResult(CSVWriter writer, boolean first, GraphDatabaseService db) {
        if (writer==null) return -1;

        String[] cols = getAllProperties(db);
        if (first) {
            writer.writeNext(cols);
        }

        String[] data = new String[cols.length];
        int count=0;
        for (Node node   : db.getAllNodes()) {
            data[0]=String.valueOf(node.getId());
            data[1]= toLabelString(node);
            for (int i = 2; i < cols.length; i++) {
                String col = cols[i];
                data[i] = node.getProperty(col,"").toString();
            }
            writer.writeNext(data);
            count++;
        }
        return count;
    }

    private String toLabelString(Node node) {
        Iterable<Label> labels = node.getLabels();
        StringBuilder sb=new StringBuilder();
        for (Label label : labels) {
            sb.append(":");
            sb.append(label.name());
        }
        return sb.toString();
    }

    private String[] getAllProperties(GraphDatabaseService db) {
        ResourceIterable<String> allProperties = db.getAllPropertyKeys();
        String[] cols = new String[(int) (Iterables.count(allProperties)+2)];
        cols[0]="id:id";
        cols[1]="id:label";
        int idx=2;
        for (String col : allProperties) cols[idx++]=col;
        return cols;
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
        return value == null ? null : value.toString();
    }

    private Map<String, Object> createParams(CSVReader reader) throws IOException {
        String[] header = reader.readNext();
        Map<String,Object> params=new LinkedHashMap<String,Object>();
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
