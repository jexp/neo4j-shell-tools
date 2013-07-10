package org.neo4j.shell.tools.imp;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.OptionDefinition;
import org.neo4j.shell.OptionValueType;
import org.neo4j.shell.kernel.apps.GraphDatabaseApp;

import java.io.*;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by mh on 10.07.13.
 */
public abstract class ImportApp extends GraphDatabaseApp {
    public static final int DEFAULT_BATCH_SIZE = 20000;
    public static final char QUOTECHAR = '"';

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

    protected String extractQuery(AppCommandParser parser) {
        String line = parser.getLineWithoutApp().trim();
        Map<String, String> options = new HashMap<String, String>(parser.options());
        while (!options.isEmpty() && line.startsWith("-")) {
            String option = options.remove(line.substring(1, 2));
            int offset = 3 + option.length();
            int idx = line.indexOf(" ", offset);
            if (idx != -1) line = line.substring(idx+1).trim();
        }
        return line;
    }

    protected CSVWriter createWriter(File outputFile, char delim, boolean quotes) throws IOException {
        if (outputFile==null) return null;
        Writer file = new BufferedWriter(new FileWriter(outputFile));
        return quotes ? new CSVWriter(file,delim, QUOTECHAR) : new CSVWriter(file,delim);
    }

    protected CSVReader createReader(File inputFile, char delim, boolean quotes) throws FileNotFoundException {
        BufferedReader reader = createReader(inputFile);
        if (reader==null) return null;
        return quotes ? new CSVReader(reader,delim, QUOTECHAR) : new CSVReader(reader,delim);
    }

    protected BufferedReader createReader(File inputFile) throws FileNotFoundException {
        if (inputFile==null) return null;
        FileReader reader = new FileReader(inputFile);
        return new BufferedReader(reader);
    }

    protected void writeRow(CSVWriter writer, String[] cols, String[] data, Map<String, Object> row) {
        for (int i = 0; i < cols.length; i++) {
            String col = cols[i];
            data[i]=row.get(col).toString();
        }
        writer.writeNext(data);
    }

    protected Map<String, Object> createParams(CSVReader reader) throws IOException {
        String[] header = reader.readNext();
        Map<String,Object> params=new LinkedHashMap<String,Object>();
        for (String name : header) {
            params.put(name,null);
        }
        return params;
    }

    protected Map<String, Object> update(Map<String, Object> params, String[] input) {
        int col=0;
        for (Map.Entry<String, Object> param : params.entrySet()) {
            param.setValue(input[col++]);
        }
        return params;
    }

    protected GraphDatabaseAPI getDb() {
        return getServer().getDb();
    }
}
