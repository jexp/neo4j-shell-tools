package org.neo4j.shell.tools.imp;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.impl.SameJvmClient;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.*;
import java.net.URL;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.Scanner;

import static org.junit.Assert.*;
import static org.neo4j.shell.tools.Asserts.assertCommand;

/**
 * Created by mh on 04.07.13.
 */
public class ImportCypherAppTest {

    private GraphDatabaseAPI db;
    private SameJvmClient client;

    @Test
    public void testExecuteCypher() throws Exception {
        assertCommand(client, "import-cypher start n=node(0) return n",
                "Query: start n=node(0) return n infile (none) delim ',' quoted false outfile (none)",
                "Import statement execution created 1 rows of output.");
    }

    @Test
    public void testRunWithOutputFile() throws Exception {
        assertCommand(client, "import-cypher -o out.csv create n return id(n) as id",
                "Query: create n return id(n) as id infile (none) delim ',' quoted false outfile out.csv"
                , "Import statement execution created 1 rows of output.");
        assertFile("id", "1");
        try (Transaction tx = db.beginTx()) {
            assertNotNull(db.getNodeById(1));
            tx.success();
        }
    }

    @Test
    public void testRunWithInputFile() throws Exception {
        createFile("in.csv", "name", "foo");
        assertCommand(client, "import-cypher -i in.csv create (n {name:{name}}) return n.name as name",
                "Query: create (n {name:{name}}) return n.name as name infile in.csv delim ',' quoted false outfile (none)",
                "finish after 1 row(s)",
                "Import statement execution created 1 rows of output.");
        try (Transaction tx = db.beginTx()) {
            assertEquals("foo",db.getNodeById(1).getProperty("name"));
            tx.success();
        }
    }

    @Test
    public void testRunWithInputWithTypesFile() throws Exception {
        createFile("in.csv", "name:string,age:int", "foo,42");
        assertCommand(client, "import-cypher -i in.csv create (n {name:{name}, age: {age}}) return n.name as name",
                "Query: create (n {name:{name}, age: {age}}) return n.name as name infile in.csv delim ',' quoted false outfile (none)",
                "finish after 1 row(s)",
                "Import statement execution created 1 rows of output.");
        try (Transaction tx = db.beginTx()) {
            Node node = db.getNodeById(1);
            assertEquals("foo", node.getProperty("name"));
            assertEquals(42, node.getProperty("age"));
            tx.success();
        }
    }
    @Test
    public void testRunWithInputFileAndReplacements() throws Exception {
        createFile("in.csv", "name,type", "foo,Bar");
        assertCommand(client, "import-cypher -i in.csv create (n:#{type} {name:{name}}) return n.name as name",
                "Query: create (n:#{type} {name:{name}}) return n.name as name infile in.csv delim ',' quoted false outfile (none)",
                "finish after 1 row(s)",
                "Import statement execution created 1 rows of output.");
        try (Transaction tx = db.beginTx()) {
            Node node = db.getNodeById(1);
            assertEquals("foo", node.getProperty("name"));
            assertEquals("Bar", IteratorUtil.single(node.getLabels()).name());
            tx.success();
        }
    }

    @Test
    public void testRunWithInputFile2() throws Exception {
        assertCommand(client, "import-cypher -d \"\\t\" -i import.csv create (n {name:{Trackmbid}}) return n.name as name",
                "Query: create (n {name:{Trackmbid}}) return n.name as name infile import.csv delim '\t' quoted false outfile (none)",
                "finish after 9 row(s)",
                "Import statement execution created 9 rows of output.");
        try (Transaction tx = db.beginTx()) {
            assertEquals("5151ffce-8617-443f-959a-82692c717cbf",db.getNodeById(1).getProperty("name"));
            tx.success();
        }
    }

    @Test
    public void testRunWithInputAndOutputFile() throws Exception {
        String[] data = {"name", "foo", "bar"};
        createFile("in.csv", data);
        assertCommand(client,"import-cypher -i in.csv -o out.csv create (n {name:{name}}) return n.name as name",
                "Query: create (n {name:{name}}) return n.name as name infile in.csv delim ',' quoted false outfile out.csv",
                "finish after 2 row(s)",
                "Import statement execution created 2 rows of output.");
        assertFile(data);
        try (Transaction tx = db.beginTx()) {
            assertEquals("foo",db.getNodeById(1).getProperty("name"));
            assertEquals("bar",db.getNodeById(2).getProperty("name"));
            tx.success();
        }
    }

    @Test
    public void testRunWithInputFileWithTabDelim() throws Exception {
        createFile("in.csv", "name\tage", "foo\t12");
        assertCommand(client,"import-cypher -d \"\\t\" -i in.csv create (n {name:{name}, age:{age}}) return n.name as name",
            "Query: create (n {name:{name}, age:{age}}) return n.name as name infile in.csv delim '\t' quoted false outfile (none)",
            "finish after 1 row(s)",
            "Import statement execution created 1 rows of output.");
        try (Transaction tx = db.beginTx()) {
            assertEquals("foo",db.getNodeById(1).getProperty("name"));
            assertEquals("12",db.getNodeById(1).getProperty("age"));
            tx.success();
        }
    }

    @Test
    public void testRunWithInputFileWithSpaceDelim() throws Exception {
        createFile("in.csv", "name age", "foo 12");
        assertCommand(client,"import-cypher -d \" \" -i in.csv create (n {name:{name}, age:{age}}) return n.name as name",
            "Query: create (n {name:{name}, age:{age}}) return n.name as name infile in.csv delim ' ' quoted false outfile (none)",
            "finish after 1 row(s)",
            "Import statement execution created 1 rows of output.");
        try (Transaction tx = db.beginTx()) {
            assertEquals("foo",db.getNodeById(1).getProperty("name"));
            assertEquals("12",db.getNodeById(1).getProperty("age"));
            tx.success();
        }
    }

    @Test
    @Ignore("Slow")
    public void testRunWithInputUrlWithTabDelim() throws Exception {
//        URL url = getClass().getResource("/in.csv");
        URL url = new URL("https://dl.dropboxusercontent.com/u/14493611/in.csv");
        assertCommand(client, "import-cypher -d \"\\t\" -i " + url + " create (n {name:{name}, age:{age}}) return n.name as name",
                "Query: create (n {name:{name}, age:{age}}) return n.name as name infile " + url + " delim '\t' quoted false outfile (none)",
                "Import statement execution created 1 rows of output.");
        try (Transaction tx = db.beginTx()) {
            assertEquals("foo",db.getNodeById(1).getProperty("name"));
            assertEquals("12",db.getNodeById(1).getProperty("age"));
            tx.success();
        }
    }

    private void createFile(String fileName, String...rows) throws IOException {
        PrintWriter inFile = new PrintWriter(new FileWriter(fileName));
        for (String row : rows) {
            inFile.println(row);
        }
        inFile.close();
    }

    @Test
    public void testRunWithOutputFileAndMultipleLines() throws Exception {
        assertCommand(client, "import-cypher -o out.csv start x=node(0,0) create n return id(n) as id",
                "Query: start x=node(0,0) create n return id(n) as id infile (none) delim ',' quoted false outfile out.csv",
                "Import statement execution created 2 rows of output.");
        assertFile("id","1","2");
        try (Transaction tx = db.beginTx()) {
            assertNotNull(db.getNodeById(1));
            assertNotNull(db.getNodeById(2));
            tx.success();
        }
    }

    @Test
    public void testToStringWithDouble() throws Exception {
        assertCommand(client, "import-cypher -o out.csv return 10000000.0001 as dec, 1000000000 as int",
                "Query: return 10000000.0001 as dec, 1000000000 as int infile (none) delim ',' quoted false outfile out.csv batch-size 1000",
                "Import statement execution created 1 rows of output.");
        assertFile("dec\",\"int", "10000000.0001\",\"1000000000");
    }

    private void assertFile(String...expected) throws FileNotFoundException {
        File file = new File("out.csv");
        assertTrue("outfile exits",file.exists());
        Scanner scanner = new Scanner(file).useDelimiter("\n");
        for (String row : expected) {
            assertEquals(quote(row),scanner.next());
        }
        assertFalse(scanner.hasNext());
    }

    private String quote(String row) {
        return '"'+row+'"';
    }

    private String readFile(File file) throws FileNotFoundException {
        return new Scanner(file).useDelimiter("\\z").next();
    }

    @Before
    public void setUp() throws RemoteException, ShellException {
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
        try (Transaction tx = db.beginTx()) {
            db.createNode();
            tx.success();
        }
        client = new SameJvmClient(Collections.<String, Serializable>emptyMap(), new GraphDatabaseShellServer(db), new TestCtrlCHandler());
    }
}
