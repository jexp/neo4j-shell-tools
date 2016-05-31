package org.neo4j.shell.tools.imp;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.impl.CollectingOutput;
import org.neo4j.shell.impl.SameJvmClient;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.*;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Created by mh on 04.07.13.
 */
@Ignore
public class ImportCypherPerformanceTest {

    public static final int ROWS = 1000000;
    private GraphDatabaseAPI db;
    private SameJvmClient client;

    @Test
    public void testRunWithInputFile() throws Exception {
        createFile("in.csv", "name", ROWS);
        long time = System.currentTimeMillis();
        assertCommand("import-cypher -i in.csv create (n {name:{name}}) return n.name as name", "Import statement execution created "+ROWS+" rows of output.");
        long delta = System.currentTimeMillis() - time;
        System.out.println("delta = " + delta);
        assertTrue("timeout 60s > "+delta,delta < TimeUnit.SECONDS.toMillis(60));
        try (Transaction tx = db.beginTx()) {
            assertEquals(String.valueOf(ROWS), db.getNodeById(ROWS-1).getProperty("name"));
            tx.success();
        }
    }

    private void createFile(String fileName, String col, int rows) throws IOException {
        PrintWriter inFile = new PrintWriter(new FileWriter(fileName));
        inFile.println(col);
        for (int row=1;row<=rows;row++) {
            inFile.println(row);
        }
        inFile.close();
    }

    private void assertCommand(String command, String expected) throws RemoteException, ShellException {
        CollectingOutput out = new CollectingOutput();
        client.evaluate(command, out);
        String output = out.asString();
        assertEquals(output+"\n should contain: "+expected,true, output.contains(expected));
    }

    @Before
    public void setUp() throws RemoteException, ShellException {
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
        client = new SameJvmClient(Collections.<String, Serializable>emptyMap(), new GraphDatabaseShellServer(db), new TestCtrlCHandler());
    }
}
