package org.neo4j.shell.tools.imp;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.impl.CollectingOutput;
import org.neo4j.shell.impl.SameJvmClient;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.*;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.Scanner;

import static org.junit.Assert.*;

/**
 * Created by mh on 04.07.13.
 */
public class ImportPerformanceTest {

    public static final int ROWS = 1000000;
    private GraphDatabaseAPI db;
    private SameJvmClient client;

    @Test
    public void testRunWithInputFile() throws Exception {
        createFile("in.csv", "name", ROWS);
        long time = System.currentTimeMillis();
        assertCommand("import -i in.csv create (n {name:{name}}) return n.name as name", "Import statement execution created "+ROWS+" rows of output.");
        long delta = System.currentTimeMillis() - time;
        System.out.println("delta = " + delta);
        assertEquals(String.valueOf(ROWS), db.getNodeById(ROWS).getProperty("name"));
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
        assertEquals(expected,out.asString().trim());
    }

    @Before
    public void setUp() throws RemoteException {
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
        client = new SameJvmClient(Collections.<String, Serializable>emptyMap(), new GraphDatabaseShellServer(db));
    }
}
