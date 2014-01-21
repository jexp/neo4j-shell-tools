package org.neo4j.shell.tools.imp;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.impl.SameJvmClient;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.shell.tools.imp.format.Config;
import org.neo4j.shell.tools.imp.format.graphml.XmlGraphMLWriter;
import org.neo4j.shell.tools.imp.util.ElementCounter;
import org.neo4j.shell.tools.imp.util.ProgressReporter;
import org.neo4j.test.TestGraphDatabaseFactory;

import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.io.StringWriter;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.Random;
import java.util.Scanner;

import static org.junit.Assert.assertEquals;
import static org.neo4j.shell.tools.Asserts.assertCommand;

/**
 * Created by mh on 04.07.13.
 */
public class ExportXmlGraphMLWriterTest {

    static final Label LABEL = DynamicLabel.label("FOO");
    static final DynamicRelationshipType TYPE = DynamicRelationshipType.withName("BAR");
    static final String TEST_XML_HEADER =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\">\n" +
            "<graph id=\"G\" edgedefault=\"directed\">\n";
    static final String TEST_XML_KEYS =
            "<key id=\"na&lt;&gt;me\" for=\"node\" attr.name=\"na&lt;&gt;me\" attr.type=\"string\"/>\n" +
            "<key id=\"count\" for=\"edge\" attr.name=\"count\" attr.type=\"int\"/>\n";
    static final String TEST_XML_DATA =
            "<node id=\"n0\" labels=\":FOO\"><data key=\"labels\">:FOO</data><data key=\"na&lt;&gt;me\">John &amp; Dö</data></node>\n" +
            "<edge id=\"e0\" source=\"n0\" target=\"n0\" label=\"BAR\"><data key=\"label\">BAR</data><data key=\"count\">0</data></edge>\n";
    static final String TEST_XML_FOOTER =
            "</graph>\n" +
            "</graphml>";

    private final Random random = new Random();
    private GraphDatabaseService db;

    @Test
    public void testExportGraphML() throws Exception {
        createData(1000);
        ProgressReporter reporter = new ProgressReporter(null, null);
        doExport(reporter, false);
        assertEquals(new ElementCounter().update(1000, 1000, 2000), reporter.getTotal());
    }

    private String doExport(ProgressReporter reporter, boolean types) throws IOException, XMLStreamException {
        try (Transaction tx = db.beginTx()) {
            StringWriter writer = new StringWriter();
            XmlGraphMLWriter xmlGraphMLWriter = new XmlGraphMLWriter();
            Config config = Config.config();
            xmlGraphMLWriter.write(new DatabaseSubGraph(db), writer, reporter, types ? config.withTypes() : config);
            tx.success();
            return writer.toString().trim();
        }
    }

    @Test
    public void testExportTinyGraph() throws Exception {
        createData(1);
        ProgressReporter reporter = new ProgressReporter(null, null);
        String xml = doExport(reporter, false);
        assertEquals(new ElementCounter().update(1, 1, 2), reporter.getTotal());
        assertEquals(TEST_XML_HEADER+TEST_XML_DATA+TEST_XML_FOOTER,xml);
    }
    @Test
    public void testExportTinyGraphWithKeys() throws Exception {
        createData(1);
        ProgressReporter reporter = new ProgressReporter(null, null);
        String xml = doExport(reporter,true);
        assertEquals(new ElementCounter().update(1, 1, 2), reporter.getTotal());
        assertEquals(TEST_XML_HEADER+TEST_XML_KEYS+TEST_XML_DATA+TEST_XML_FOOTER,xml);
    }

    private void createData(int nodes) {
        try (Transaction tx = db.beginTx()) {
            for (int i=0;i< nodes;i++) {
                Node node = db.createNode(LABEL);
                node.setProperty("na<>me","John & Dö");
            }
            for (int i=0;i< nodes;i++) {
                Node from = db.getNodeById(random.nextInt(nodes));
                Node to = db.getNodeById(random.nextInt(nodes));
                Relationship rel = from.createRelationshipTo(to, TYPE);
                rel.setProperty("count",i);
            }
            tx.success();
        }
    }

    @Before
    public void setUp() throws RemoteException, ShellException {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
    }
}
