package org.neo4j.shell.tools.imp;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.impl.SameJvmClient;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.*;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.Random;

import static org.junit.Assert.assertTrue;
import static org.neo4j.shell.tools.Asserts.assertCommand;
import static org.neo4j.shell.tools.imp.ExportXmlGraphMLWriterTest.LABEL;
import static org.neo4j.shell.tools.imp.ExportXmlGraphMLWriterTest.TYPE;

/**
 * Created by efulton on 5/9/16.
 */
public class ExportBinaryAppTest {

    private final Random random = new Random(1);
    private GraphDatabaseAPI db;
    private SameJvmClient client;

    @Test
    public void testExportKryo() throws Exception {
        createData(1000);
        assertCommand(client, "export-binary -o target/kryo-export.bin", "Wrote to binary file target/kryo-export.bin 0. 100%: nodes = 1000 rels = 1000 properties = 2000 time");
        InputStream realStream = getClass().getResourceAsStream("/kryo-export.bin");
        InputStream testStream = new FileInputStream("target/kryo-export.bin");
        assertTrue(streamsAreEqual(realStream, testStream));
    }

    @Test
    public void testExportTinyKryoGraph() throws Exception {
        createData(1);
        assertCommand(client, "export-binary -o target/tiny-kryo-export.bin", "Wrote to binary file target/tiny-kryo-export.bin 0. 100%: nodes = 1 rels = 1 properties = 2 time");

        InputStream realStream = getClass().getResourceAsStream("/tiny-kryo-export.bin");
        InputStream testStream = new FileInputStream("target/tiny-kryo-export.bin");
        assertTrue(streamsAreEqual(realStream, testStream));
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

    private boolean streamsAreEqual(InputStream input1, InputStream input2) throws IOException {
        if (!(input1 instanceof BufferedInputStream)) {
            input1 = new BufferedInputStream(input1);
        }
        if (!(input2 instanceof BufferedInputStream)) {
            input2 = new BufferedInputStream(input2);
        }

        int ch = input1.read();
        while (-1 != ch) {
            int ch2 = input2.read();
            if (ch != ch2) {
                return false;
            }
            ch = input1.read();
        }

        int ch2 = input2.read();
        return (ch2 == -1);
    }

    @Before
    public void setUp() throws RemoteException, ShellException {
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
        client = new SameJvmClient(Collections.<String, Serializable>emptyMap(), new GraphDatabaseShellServer(db), new TestCtrlCHandler());
    }
}