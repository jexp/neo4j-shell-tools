package org.neo4j.shell.tools.imp;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.impl.SameJvmClient;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.neo4j.shell.tools.Asserts.assertCommand;

/**
 * Created by efulton on 5/9/16.
 */
public class ImportBinaryAppTest {

    private GraphDatabaseAPI db;
    private SameJvmClient client;

    @Test
    public void testRunWithInputFile() throws Exception {
        String path = getClass().getResource("/kryo-export.bin").getPath();
        runImport(path);
    }

    private void runImport(Object path) throws RemoteException, ShellException {
        assertCommand(client,"import-binary -i " + path+" -b 20000 -r UNKNOWN -c",
            "Binary import file "+path+" rel-type UNKNOWN batch-size 20000 use disk-cache true",
            "Importing Indices and Constraints",
            "Index Import Complete",
            "Importing Nodes and Edges",
            "Import Complete",
            "Binary import created 2000 entities.",
            "finish after 2000 row(s)  0. 100%: nodes = 1000 rels = 1000 properties = 2000");
        try (Transaction tx = db.beginTx()) {
            assertEquals("John & Dö", db.getNodeById(0).getProperty("na<>me"));
            tx.success();
        }
    }

    @Before
    public void setUp() throws RemoteException, ShellException {
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
        client = new SameJvmClient(Collections.<String, Serializable>emptyMap(), new GraphDatabaseShellServer(db), new TestCtrlCHandler());
    }
}