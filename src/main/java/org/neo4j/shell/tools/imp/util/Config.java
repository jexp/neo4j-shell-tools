package org.neo4j.shell.tools.imp.util;

import org.neo4j.shell.AppCommandParser;

import java.util.HashMap;
import java.util.Map;

/**
 * @author mh
 * @since 19.01.14
 */
public class Config {
    public static final char QUOTECHAR = '"';
    //    public static final int DEFAULT_BATCH_SIZE = 20000;
    public static final int DEFAULT_BATCH_SIZE = 1000; // work around cypher bug
    public static final String DEFAULT_DELIM = ",";

    private int batchSize = DEFAULT_BATCH_SIZE;
    private boolean silent = false;
    private String delim = DEFAULT_DELIM;
    private boolean quotes;
    private boolean types = false;

    public static String extractQuery(AppCommandParser parser) {
        String line = parser.getLineWithoutApp().trim();
        Map<String, String> options = new HashMap<String, String>(parser.options());
        while (!options.isEmpty() && line.startsWith("-")) {
            String option = options.remove(line.substring(1, 2));
            int offset = option!=null ? 3 + option.length() : 2;
            if (option!=null && !option.isEmpty() && option.trim().isEmpty()) offset+=2; // for quoted space or tab
            int idx = line.indexOf(" ", offset);
            if (idx != -1) line = line.substring(idx+1).trim();
            else if (offset >= line.trim().length()) line = "";
        }
        return line.trim();
    }

    public int getBatchSize() {
        return batchSize;
    }

    public boolean isSilent() {
        return silent;
    }

    public char getDelimChar() {
        return delim.charAt(0);
    }

    public String getDelim() {
        return delim;
    }

    public boolean isQuotes() {
        return quotes;
    }

    public boolean useTypes() {
        return types;
    }

    public static Config fromOptions(AppCommandParser parser) {
        Config config = new Config();
        config.silent = parser.options().containsKey("s");
        config.batchSize = parser.optionAsNumber("b", DEFAULT_BATCH_SIZE).intValue();
        config.delim = delim(parser.option("d", String.valueOf(DEFAULT_DELIM)));
        config.quotes = parser.options().containsKey("q");
        config.types = parser.options().containsKey("t");
        return config;
    }

    private static String delim(String value) {
        if (value.length()==1) return value;
        if (value.contains("\\t")) return String.valueOf('\t');
        if (value.contains(" ")) return " ";
        throw new RuntimeException("Illegal delimiter '"+value+"'");
    }

    public static Config config() {
        return new Config();
    }

    public Config withTypes() {
        this.types=true;
        return this;
    }
}
