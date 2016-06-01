package org.neo4j.shell.tools.imp;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.*;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.shell.impl.SystemOutput;
import org.neo4j.shell.tools.imp.format.graphml.XmlGraphMLReader;
import org.neo4j.shell.tools.imp.util.*;

import java.io.File;

/**
 * Using Enron Data set from Chris Diehl
 * http://www.infochimps.com/datasets/enron-email-data-with-manager-subordinate-relationship-metadata
 */
@Ignore("slow")
public class GraphMLReaderPerformanceTest {

    public static final int MEGABYTE = 1024 * 1024;

    private GraphDatabaseService db;
    private XmlGraphMLReader graphMlReader;
    private CountingReader reader;
    private File directory = new File("target/enron.db");
    private File graphMlFile = new File("test-data/Enron_Dataset_v0.12.graphml");

    @Before
    public void setUp() throws Exception {
        FileUtils.deleteRecursively(directory);
        db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(directory.getAbsolutePath()))
                .setConfig(GraphDatabaseSettings.pagecache_memory,"350M")
                .setConfig(GraphDatabaseSettings.keep_logical_logs, "false")
                .newGraphDatabase();

        reader = new CountingReader(graphMlFile);
        graphMlReader = new XmlGraphMLReader(db).reporter(new ProgressReporter(reader,new SystemOutput()));
    }

    @Test
    public void testReadEnronData() throws Exception {
        NodeCache cache = MapNodeCache.<String, Long>usingMapDb();
        this.graphMlReader.parseXML(reader, cache);
    }

    @After
    public void tearDown() throws Exception {
        if (db!=null) db.shutdown();
        if (reader!=null) reader.close();
    }

}
