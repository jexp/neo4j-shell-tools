package org.neo4j.shell.tools.imp;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.impl.SameJvmClient;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.shell.tools.imp.format.MultiStatementCypherSubGraphExporterTest;
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
import static org.neo4j.shell.tools.imp.format.MultiStatementCypherSubGraphExporterTest.CLEANUP_SECTION;
import static org.neo4j.shell.tools.imp.format.MultiStatementCypherSubGraphExporterTest.CONSTRAINT_SECTION;
import static org.neo4j.shell.tools.imp.format.MultiStatementCypherSubGraphExporterTest.createData;

/**
 * @author mh
 * @since 04.07.13
 */
public class ExportCypherAppTest {

    private GraphDatabaseAPI db;
    private SameJvmClient client;

    @Test
    public void testExportCypher() throws Exception {
        createData(1000,db);
        assertCommand(client, "export-cypher -o target/test.cypher",
                null,null,"Wrote to Cypher-file target/test.cypher 2. 100%: nodes = 1000 rels = 2997 properties = 999 time");
    }

    @Test
    public void testExportTinyGraph() throws Exception {
        createData(1,db);
        assertCommand(client, "export-cypher -o target/test.cypher",
                null,
                "Wrote to Cypher-file target/test.cypher 1. 100%: nodes = 1 rels = 0 properties = 0 time");
        String fileContent = new Scanner(new File("target/test.cypher")).useDelimiter("\\Z").next();
        assertEquals(
                "begin\n" +
                "CREATE (:`UNIQUE IMPORT LABEL` {`UNIQUE IMPORT ID`:0});\n" +
                "commit\n" +
                CONSTRAINT_SECTION +
                 CLEANUP_SECTION
                ,fileContent+"\n");
    }

    @Test
    public void testExportTinyGraphWithCypher() throws Exception {
        createData(2,db);
        assertCommand(client, "export-cypher -o target/test.cypher match(n) return n",
                null,"Wrote to Cypher-file target/test.cypher 1. 100%: nodes = 2 rels = 2 properties = 0 time");
        String fileContent = new Scanner(new File("target/test.cypher")).useDelimiter("\\Z").next();
        assertEquals(
                "begin\n" +
                        "CREATE (:`UNIQUE IMPORT LABEL` {`UNIQUE IMPORT ID`:0});\n" +
                        "CREATE (:`User` {`age`:43, `name`:\"User1\"});\n" +
                        "commit\n" +
                        CONSTRAINT_SECTION +
                        CLEANUP_SECTION
                ,fileContent+"\n");
    }

    @Test
    public void testExportTinyGraphWithCypherAndRelsInBetween() throws Exception {
        createData(2,db);
        assertCommand(client, "export-cypher -r -o target/test.cypher match (n)-[r]->() return n,r",
                null,null,"Wrote to Cypher-file target/test.cypher 2. 100%: nodes = 2 rels = 3 properties = 1 time");
        String fileContent = new Scanner(new File("target/test.cypher")).useDelimiter("\\Z").next();
        System.out.println(fileContent);
        assertEquals(
                "begin\n" +
                "CREATE (:`UNIQUE IMPORT LABEL` {`UNIQUE IMPORT ID`:0});\n" +
                "CREATE (:`User` {`age`:43, `name`:\"User1\"});\n" +
                "commit\n" +
                CONSTRAINT_SECTION +
                "begin\n" +
                "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`User`{`name`:\"User1\"}) CREATE (n1)-[:`KNOWS` {`since`:2011}]->(n2);\n"+
                "commit\n" +
                CLEANUP_SECTION
                ,fileContent+"\n");
    }

    @Before
    public void setUp() throws RemoteException, ShellException {
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
        client = new SameJvmClient(Collections.<String, Serializable>emptyMap(), new GraphDatabaseShellServer(db), new TestCtrlCHandler());
    }
}
