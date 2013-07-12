package org.neo4j.shell.tools.imp;

import org.junit.Before;
import org.junit.Test;
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
    public void testRunWithInputUrl() throws Exception {
        URL path = getClass().getResource("/graphml-with-attributes.xml");
        runImport(path);
    }

    private void runImport(Object path) throws RemoteException, ShellException {
        assertCommand(client,"import-graphml -i " + path+" -b 20000 -t UNKNOWN -c",
                "GraphML-Import file "+path+" rel-type UNKNOWN batch-size 20000 use disk-cache true",
                "", "",
                "GraphML import created 13 entities.");
        assertEquals("green", db.getNodeById(1).getProperty("color"));
        assertEquals(1.0, db.getRelationshipById(0).getProperty("weight"));
        assertEquals("UNKNOWN", db.getRelationshipById(0).getType().name());
    }

    @Before
    public void setUp() throws RemoteException {
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
        client = new SameJvmClient(Collections.<String, Serializable>emptyMap(), new GraphDatabaseShellServer(db));
    }
}
