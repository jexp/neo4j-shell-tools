package org.neo4j.shell.tools.imp;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.impl.SameJvmClient;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.Random;
import java.util.Scanner;

import static org.junit.Assert.assertEquals;
import static org.neo4j.shell.tools.Asserts.assertCommand;

/**
 * Created by mh on 04.07.13.
 */
public class ExportGraphMLAppTest {

    public static final Label LABEL = DynamicLabel.label("FOO");
    public static final DynamicRelationshipType TYPE = DynamicRelationshipType.withName("BAR");
    private static final String TEST_XML_HEADER =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\"\n" +
            " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
            " xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns\n" +
            " http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\">\n" +
            "<graph id=\"G\" edgedefault=\"directed\">\n";
    private static final String TEST_XML_KEYS =
            "<key id=\"name\" for=\"node\" attr.name=\"name\" attr.type=\"string\"/>\n" +
            "<key id=\"count\" for=\"edge\" attr.name=\"count\" attr.type=\"int\"/>\n";
    private static final String TEST_XML_DATA =
            "<node id=\"n0\" labels=\"FOO\" ><data key=\"labels\">FOO</data><data key=\"name\">John Doe</data></node>\n" +
            "<edge id=\"e0\" source=\"n0\" target=\"n0\" label=\"BAR\"><data key=\"label\">BAR</data><data key=\"count\">0</data></edge>\n";
    private static final String TEST_XML_FOOTER =
            "</graph>\n" +
            "</graphml>";

    private final Random random = new Random();
    private GraphDatabaseAPI db;
    private SameJvmClient client;

    @Test
    public void testExportGraphML() throws Exception {
        createData(1000);
        assertCommand(client, "export-graphml -t -o target/test.graphml",
                "Wrote to GraphML-file target/test.graphml 0. 100%: nodes = 1000 rels = 1000 properties = 2000 time");
    }

    @Test
    public void testExportTinyGraph() throws Exception {
        createData(1);
        assertCommand(client, "export-graphml -o target/test.xml",
                "Wrote to GraphML-file target/test.xml 0. 100%: nodes = 1 rels = 1 properties = 2 time");
        String fileContent = new Scanner(new File("target/test.xml")).useDelimiter("\\Z").next();
        assertEquals(TEST_XML_HEADER+TEST_XML_DATA+TEST_XML_FOOTER,fileContent);
    }
    @Test
    public void testExportTinyGraphWithKeys() throws Exception {
        createData(1);
        assertCommand(client, "export-graphml -t -o target/test.xml",
                "Wrote to GraphML-file target/test.xml 0. 100%: nodes = 1 rels = 1 properties = 2 time");
        String fileContent = new Scanner(new File("target/test.xml")).useDelimiter("\\Z").next();
        assertEquals(TEST_XML_HEADER+TEST_XML_KEYS+TEST_XML_DATA+TEST_XML_FOOTER,fileContent);
    }

    private void createData(int nodes) {
        try (Transaction tx = db.beginTx()) {
            for (int i=0;i< nodes;i++) {
                Node node = db.createNode(LABEL);
                node.setProperty("name","John Doe");
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
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
        client = new SameJvmClient(Collections.<String, Serializable>emptyMap(), new GraphDatabaseShellServer(db));
    }
}
