package org.neo4j.shell.tools.imp.format;

import org.neo4j.shell.AppCommandParser;

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
