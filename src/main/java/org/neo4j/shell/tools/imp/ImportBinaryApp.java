package org.neo4j.shell.tools.imp;

import com.esotericsoftware.kryo.io.Input;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.shell.*;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.shell.tools.imp.format.kryo.KryoReader;
import org.neo4j.shell.tools.imp.util.*;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.InflaterInputStream;

/**
 * Created by efulton on 5/5/16.
 */
public class ImportBinaryApp extends BinaryApp {

    private static final int DEFAULT_BATCH_SIZE = 10000;
    private static final String DEFAULT_REL_TYPE = "RELATED_TO";

    {
        addOptionDefinition("i", new OptionDefinition(OptionValueType.MUST, "Input Binary file"));
        addOptionDefinition("b", new OptionDefinition(OptionValueType.MAY, "Batch Size. Default is: " + DEFAULT_BATCH_SIZE));
        addOptionDefinition("r", new OptionDefinition(OptionValueType.MAY, "Default Relationship-Type otherwise edge attribute 'label' " + DEFAULT_REL_TYPE));
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
        String relType = parser.option("r", DEFAULT_REL_TYPE);
        int batchSize = Integer.parseInt( parser.option("b", String.valueOf(DEFAULT_BATCH_SIZE)));
        boolean diskSpillCache = parser.options().containsKey("c");

        try (FileInputStream fileInputStream = new FileInputStream(fileName)) {
            String type = loadType(fileInputStream);
            switch (type) {
                case KRYO:
                    readKryo(fileName, fileInputStream, out, diskSpillCache, batchSize, relType);
                    break;
                case CBOR:
                    throw new RuntimeException(String.format("%s is not supported yet", CBOR));
                case PACKSTREAM:
                    throw new RuntimeException(String.format("%s is not supported yet", PACKSTREAM));
            }
        }

        return Continuation.INPUT_COMPLETE;
    }

    private String loadType(FileInputStream fileInputStream) throws IOException {
        StringBuilder type = new StringBuilder();
        String result = null;
        for (int i = 0; i < MAX_FORMAT_STRING_SIZE && fileInputStream.available() > 0; i++) {
            char character = (char) fileInputStream.read();
            type.append(character);
            if (BINARY_FORMATS.contains(type.toString())) {
                result = type.toString();
                break;
            }
        }
        return result;
    }

    private void readKryo(String fileName, FileInputStream fileInputStream, Output out, boolean diskSpillCache, int batchSize, String relType) throws IOException {
        Input input = new Input(new InflaterInputStream(new BufferedInputStream(fileInputStream,FileUtils.MEGABYTE)),FileUtils.MEGABYTE);
        FileInputWrapper fileInputWrapper = new FileInputWrapper(fileInputStream);
        out.println(String.format("Binary import file %s rel-type %s batch-size %d use disk-cache %s", fileName, relType, batchSize, diskSpillCache));
        ProgressReporter reporter = new ProgressReporter(fileInputWrapper, out);
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
}
