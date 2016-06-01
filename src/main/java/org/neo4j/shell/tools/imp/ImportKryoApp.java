package org.neo4j.shell.tools.imp;

import com.esotericsoftware.kryo.io.Input;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.shell.*;
import org.neo4j.shell.impl.AbstractApp;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.shell.tools.imp.format.kryo.KryoReader;
import org.neo4j.shell.tools.imp.util.*;

import java.io.File;
import java.io.FileInputStream;

/**
 * Created by efulton on 5/5/16.
 */
public class ImportKryoApp extends AbstractApp {

    private static final int DEFAULT_BATCH_SIZE = 10000;
    private static final String DEFAULT_REL_TYPE = "RELATED_TO";

    {
        addOptionDefinition("i", new OptionDefinition(OptionValueType.MUST, "Input Binary file"));
        addOptionDefinition("b", new OptionDefinition(OptionValueType.MUST, "Batch Size default " + DEFAULT_BATCH_SIZE));
        addOptionDefinition("r", new OptionDefinition(OptionValueType.MUST, "Default Relationship-Type otherwise edge attribute 'label' " + DEFAULT_REL_TYPE));
        addOptionDefinition("c", new OptionDefinition(OptionValueType.NONE, "Use a node-cache that overflows to disk, necessary for very large imports"));
    }

    @Override
    public String getName() {
      return "import-binary";
    }

    @Override
    public GraphDatabaseShellServer getServer() {
        return (GraphDatabaseShellServer) super.getServer();
    }

    @Override
    public Continuation execute(AppCommandParser parser, Session session, Output out) throws Exception {
        String fileName = parser.option("i", null);
        File file = new File(fileName);
        Input input = new Input(new FileInputStream(fileName));
        KryoInputWrapper kryoInputWrapper = new KryoInputWrapper(input, file.length());
        int batchSize = Integer.parseInt(parser.option("b", String.valueOf(DEFAULT_BATCH_SIZE)));
        String relType = parser.option("r", DEFAULT_REL_TYPE);
        boolean diskSpillCache = parser.options().containsKey("c");
        if (input != null) {
            out.println(String.format("Binary import file %s rel-type %s batch-size %d use disk-cache %s", fileName, relType, batchSize, diskSpillCache));
            ProgressReporter reporter = new ProgressReporter(kryoInputWrapper, out);
            GraphDatabaseService db = getServer().getDb();
            NodeCache cache = diskSpillCache ? MapNodeCache.<Long, Long>usingMapDb() : MapNodeCache.<Long, Long>usingHashMap();
            try(BatchTransaction tx = new BatchTransaction(db, batchSize, reporter)) {
                KryoReader importReader = new KryoReader(db, relType, reporter, out);
                long count = importReader.readBinaryDump(input, tx, cache);
                out.println("Binary import created " + count + " entities.");
            } finally {
                input.close();
            }
        }
        return Continuation.INPUT_COMPLETE;
    }
}