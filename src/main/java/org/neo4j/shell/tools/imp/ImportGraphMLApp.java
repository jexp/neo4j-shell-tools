package org.neo4j.shell.tools.imp;

import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.shell.*;
import org.neo4j.shell.kernel.apps.GraphDatabaseApp;
import org.neo4j.shell.tools.imp.util.*;

import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.IOException;

/**
 * Created by mh on 04.07.13.
 */
public class ImportGraphMLApp extends GraphDatabaseApp {

    private static final int DEFAULT_BATCH_SIZE = 40000;

    {
        addOptionDefinition("i", new OptionDefinition(OptionValueType.MUST,
                "Input GraphML file"));
        addOptionDefinition("b", new OptionDefinition(OptionValueType.MUST,
                "Batch Size default " + DEFAULT_BATCH_SIZE));
        addOptionDefinition("t", new OptionDefinition(OptionValueType.MUST,
                "Default Relationship-Type otherwise edge attribute 'label' " + DEFAULT_REL_TYPE));
        addOptionDefinition("c", new OptionDefinition(OptionValueType.NONE,
                "Use a node-cache that overflows to disk, necessary for very large imports"));
    }

    private static final String DEFAULT_REL_TYPE = "RELATED_TO";

    @Override
    public String getName() {
        return "import-graphml";
    }

    @Override
    protected Continuation exec(AppCommandParser parser, Session session, Output out) throws Exception {
        File file = fileFor(parser, "i");
        int batchSize = Integer.parseInt(parser.option("b", String.valueOf(DEFAULT_BATCH_SIZE)));
        String relType = parser.option("t", DEFAULT_REL_TYPE);
        boolean diskSpillCache = parser.options().containsKey("c");
        if (file != null) {
            out.println(String.format("GraphML-Import file %s rel-type %s batch-size %d use disk-cache %s",file.getAbsoluteFile(),relType,batchSize,diskSpillCache));
            long count = execute(file, batchSize, relType, diskSpillCache, out);
            out.println("GraphML import created " + count + " entities.");
        }
        return Continuation.INPUT_COMPLETE;
    }

    protected File fileFor(AppCommandParser parser, String option) {
        String fileName = parser.option(option, null);
        if (fileName == null) return null;
        File file = new File(fileName);
        if (file.exists() && file.canRead() && file.isFile()) return file;
        return null;
    }

    private long execute(File file, int batchSize, String relType, boolean diskSpillCache, Output out) throws XMLStreamException, IOException {
        CountingReader reader = new CountingReader(file);
        try {
            GraphDatabaseAPI db = getServer().getDb();
            ProgressReporter reporter = new ProgressReporter(reader, out);
            GraphMLReader graphMLReader = new GraphMLReader(db)
                    .batchSize(batchSize).relType(relType)
                    .reporter(reporter);
            NodeCache cache = diskSpillCache ? MapNodeCache.usingMapDb() : MapNodeCache.usingHashMap();
            return graphMLReader.parseXML(reader, cache);
        } finally {
            reader.close();
        }
    }

}
