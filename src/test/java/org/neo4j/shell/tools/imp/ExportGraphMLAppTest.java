package org.neo4j.shell.tools.imp;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
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
import static org.neo4j.shell.tools.imp.ExportXmlGraphMLWriterTest.*;
/**
 * Created by mh on 04.07.13.
 */
public class ExportGraphMLAppTest {

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
    public void testExportTinyGraphWithCypher() throws Exception {
        createData(1);
        assertCommand(client, "export-graphml -o target/test.xml match(n) return n",
                "Wrote to GraphML-file target/test.xml 0. 100%: nodes = 1 rels = 0 properties = 1 time");
        String fileContent = new Scanner(new File("target/test.xml")).useDelimiter("\\Z").next();
        assertEquals(TEST_XML_HEADER+TEST_XML_DATA_NODE+TEST_XML_FOOTER,fileContent);
    }

    @Test
    public void testExportTinyGraphWithCypherAndRelsInBetween() throws Exception {
        createData(1);
        assertCommand(client, "export-graphml -r -o target/test.xml match (n)-[r]->() return n,r",
                "Wrote to GraphML-file target/test.xml 0. 100%: nodes = 1 rels = 1 properties = 2 time");
        String fileContent = new Scanner(new File("target/test.xml")).useDelimiter("\\Z").next();
        assertEquals(TEST_XML_HEADER+TEST_XML_DATA+TEST_XML_FOOTER,fileContent);
    }
    @Test
    public void testExportTinyGraphWithCypherAndRelsInBetweenAndKeyTypes() throws Exception {
        createData(1);
        assertCommand(client, "export-graphml -t -r -o target/test.xml match (n:FOO)-[r]->() return n,r",
                "Wrote to GraphML-file target/test.xml 0. 100%: nodes = 1 rels = 1 properties = 2 time");
        String fileContent = new Scanner(new File("target/test.xml")).useDelimiter("\\Z").next();
        assertEquals(TEST_XML_HEADER+TEST_XML_KEYS+TEST_XML_DATA+TEST_XML_FOOTER,fileContent);
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
                node.setProperty("na<>me","John & Dö");
            }
            for (int i=0;i< nodes;i++) {
                Node from = db.getNodeById(random.nextInt(nodes));
                Node to = db.getNodeById(random.nextInt(nodes));
                Relationship rel = from.createRelationshipTo(to, TYPE);
                rel.setProperty("count",((double)i+1)/100);
            }
            tx.success();
        }
    }

    @Before
    public void setUp() throws RemoteException, ShellException {
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
        client = new SameJvmClient(Collections.<String, Serializable>emptyMap(), new GraphDatabaseShellServer(db), new TestCtrlCHandler());
    }
}
