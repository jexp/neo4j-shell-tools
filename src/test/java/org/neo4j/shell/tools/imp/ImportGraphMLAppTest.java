package org.neo4j.shell.tools.imp;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.impl.SameJvmClient;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.Serializable;
import java.net.URL;
import java.rmi.RemoteException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.neo4j.shell.tools.Asserts.assertCommand;

/**
 * Created by mh on 04.07.13.
 */
public class ImportGraphMLAppTest {

    private GraphDatabaseAPI db;
    private SameJvmClient client;

    @Test
    public void testRunWithInputFile() throws Exception {
        String path = getClass().getResource("/graphml-with-attributes.xml").getPath();
        runImport(path);

    }
    @Test
    public void testRunWithYedFile() throws Exception {
        String path = getClass().getResource("/yed.xml").getPath();
        assertCommand(client,"import-graphml -t -i " + path +" -b 20000 -r UNKNOWN -c",
                "GraphML-Import file "+ path +" rel-type UNKNOWN batch-size 20000 use disk-cache true",
                "finish after 1 row(s)",
                "GraphML import created 1 entities.");
        try (Transaction tx = db.beginTx()) {
            Node node = db.getNodeById(0);
            for (String prop : node.getPropertyKeys()) {
                System.out.println("prop = " + prop+" "+node.getProperty(prop));
            }
//            assertEquals("d1", node.getProperty("value"));
            tx.success();
        }
    }

    @Test
    public void testRunWithLabelsAndInputFile() throws Exception {
        String path = getClass().getResource("/graphml-with-labels.xml").getPath();
        runImport(path);
        try (Transaction tx = db.beginTx()) {
            assertEquals(true, db.getNodeById(0).hasLabel(DynamicLabel.label("Color")));
            assertEquals(true, db.getNodeById(2).hasLabel(DynamicLabel.label("Color")));
            assertEquals(true, db.getNodeById(5).hasLabel(DynamicLabel.label("Color")));
            assertEquals(true, db.getNodeById(5).hasLabel(DynamicLabel.label("Highlight")));
        }
    }
    @Test
    public void testRunWithInputUrl() throws Exception {
        URL path = getClass().getResource("/graphml-with-attributes.xml");
        runImport(path);
    }

    private void runImport(Object path) throws RemoteException, ShellException {
        assertCommand(client,"import-graphml -t -i " + path+" -b 20000 -r UNKNOWN -c",
                "GraphML-Import file "+path+" rel-type UNKNOWN batch-size 20000 use disk-cache true",
                "finish after 13 row(s)",
                "GraphML import created 13 entities.");
        try (Transaction tx = db.beginTx()) {
            assertEquals("green", db.getNodeById(0).getProperty("color"));
            assertEquals("tur&uoise", db.getNodeById(5).getProperty("color"));
            assertEquals(1.0, db.getRelationshipById(0).getProperty("weight"));
            assertEquals("UNKNOWN", db.getRelationshipById(0).getType().name());
            tx.success();
        }
    }

    @Before
    public void setUp() throws RemoteException, ShellException {
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
        client = new SameJvmClient(Collections.<String, Serializable>emptyMap(), new GraphDatabaseShellServer(db), new TestCtrlCHandler());
    }
}
