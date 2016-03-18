package org.neo4j.shell.tools.imp.format;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.graphdb.*;
import org.neo4j.shell.tools.imp.util.ProgressReporter;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.*;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.MapUtil.map;

/**
 * @author mh
 * @since 18.01.14
 */
public class MultiStatementCypherSubGraphExporterTest {

    public static final Label USER = label("User");
    public static final String CONSTRAINT_END_SECTION = "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;\n" +
            "commit\n" +
            "schema await\n";
    public static final String CONSTRAINT_SECTION = "begin\n" +
            "CREATE INDEX ON :`User`(`age`);\n" +
            "CREATE CONSTRAINT ON (node:`User`) ASSERT node.`name` IS UNIQUE;\n" +
            CONSTRAINT_END_SECTION;
    public static final String CLEANUP_SECTION = "begin\n" +
            "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 1000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;\n" +
            "commit\n" +
            "begin\n" +
            "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;\n" +
            "commit\n";
    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
    }

    @After
    public void tearDown() throws Exception {
        db.shutdown();
    }

    public static void createData(int count, GraphDatabaseService db) {
        createData(count,db, Collections.<String,Object>emptyMap());
    }

    public static void createData(int count, GraphDatabaseService db, Map<String,Object> moreProps) {
        try (Transaction tx = db.beginTx()) {
            db.schema().constraintFor(USER).assertPropertyIsUnique("name").create();
            db.schema().indexFor(USER).on("age").create();
            tx.success();
        }
        Transaction tx = db.beginTx();
        Node n1 = db.createNode();
        Node n2;
        for (int i=1;i<count;i++) {
            n2 = i % 2 == 0 ? db.createNode() : db.createNode(USER);
            n2.setProperty("name","User"+i);
            n2.setProperty("age",42+i);
            for (Map.Entry<String, Object> entry : moreProps.entrySet()) {
                n2.setProperty(entry.getKey(),entry.getValue());
            }
            Relationship rel = n1.createRelationshipTo(n2, DynamicRelationshipType.withName("KNOWS"));
            rel.setProperty("since",2010+i);
            n1 = n2;
            if (i % 10000 == 0) {
                tx.success();
                tx.close();
                tx = db.beginTx();
            }
        }
        tx.success();
        tx.close();
    }

    @Test
    public void testFormatStringsAndNumbers() throws Exception {
        try (Transaction tx = db.beginTx()) {
            Node n = db.createNode(USER);
            n.setProperty("foo","a\n\b\t\"c");
            n.setProperty("long",10000000000L);
            n.setProperty("double",10000000000.0001D);
            tx.success();
        }
        String output = doOutput(db, 1000);
        Assert.assertEquals(
                        "begin\n" +
                        "CREATE (:`User`:`UNIQUE IMPORT LABEL` {`double`:10000000000.0001, `foo`:\"a\\n\b\\t\\\"c\", `long`:10000000000, `UNIQUE IMPORT ID`:0});\n" +
                        "commit\n"+
                        "begin\n"+
                        CONSTRAINT_END_SECTION +
                        CLEANUP_SECTION,output);
    }
    @Test
    public void testExportSingleNodeGraph() throws Exception {
        createData(1, db);
        String output = doOutput(db, 1000);
        Assert.assertEquals(
                "begin\n" +
                        "CREATE (:`UNIQUE IMPORT LABEL` {`UNIQUE IMPORT ID`:0});\n" +
                        "commit\n" +
                        CONSTRAINT_SECTION +
                        CLEANUP_SECTION,output);
    }
    @Test
    public void testExportProperties() throws Exception {
        createData(2, db, map("kids",new String[]{"Jane","Jake"},"married",true,"height",1.82));
        String output = doOutput(db, 1000);
        Assert.assertEquals(
                "begin\n" +
                        "CREATE (:`UNIQUE IMPORT LABEL` {`UNIQUE IMPORT ID`:0});\n" +
                        "CREATE (:`User` {`age`:43, `name`:\"User1\", `height`:1.82, `kids`:[\"Jane\", \"Jake\"], `married`:true});\n" +
                        "commit\n" +
                        CONSTRAINT_SECTION +
                        "begin\n" +
                        "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`User`{`name`:\"User1\"}) CREATE (n1)-[:`KNOWS` {`since`:2011}]->(n2);\n" +
                        "commit\n" +
                        CLEANUP_SECTION,output);
    }

    @Test
    public void testExportSingleRelationshipGraph() throws Exception {
        createData(2, db);
        String output = doOutput(db, 1000);
        Assert.assertEquals(
                "begin\n" +
                        "CREATE (:`UNIQUE IMPORT LABEL` {`UNIQUE IMPORT ID`:0});\n" +
                        "CREATE (:`User` {`age`:43, `name`:\"User1\"});\n" +
                        "commit\n" +
                        CONSTRAINT_SECTION +
                        "begin\n" +
                        "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`User`{`name`:\"User1\"}) CREATE (n1)-[:`KNOWS` {`since`:2011}]->(n2);\n" +
                        "commit\n" +
                        CLEANUP_SECTION, output);

    }
    @Test
    public void testExportBatches() throws Exception {
        createData(5, db);
        String output = doOutput(db, 2);
        System.out.println(output);
        Assert.assertEquals(
                "begin\n" +
                        "CREATE (:`UNIQUE IMPORT LABEL` {`UNIQUE IMPORT ID`:0});\n" +
                        "CREATE (:`User` {`age`:43, `name`:\"User1\"});\n" +
                        "commit\n" +
                        "begin\n" +
                        "CREATE (:`UNIQUE IMPORT LABEL` {`age`:44, `name`:\"User2\", `UNIQUE IMPORT ID`:2});\n" +
                        "CREATE (:`User` {`age`:45, `name`:\"User3\"});\n" +
                        "commit\n" +
                        "begin\n" +
                        "CREATE (:`UNIQUE IMPORT LABEL` {`age`:46, `name`:\"User4\", `UNIQUE IMPORT ID`:4});\n" +
                        "commit\n" +
                        CONSTRAINT_SECTION +
                        "begin\n" +
                        "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`User`{`name`:\"User1\"}) CREATE (n1)-[:`KNOWS` {`since`:2011}]->(n2);\n" +
                        "MATCH (n1:`User`{`name`:\"User1\"}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:2}) CREATE (n1)-[:`KNOWS` {`since`:2012}]->(n2);\n" +
                        "commit\n" +
                        "begin\n" +
                        "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:2}), (n2:`User`{`name`:\"User3\"}) CREATE (n1)-[:`KNOWS` {`since`:2013}]->(n2);\n" +
                        "MATCH (n1:`User`{`name`:\"User3\"}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:4}) CREATE (n1)-[:`KNOWS` {`since`:2014}]->(n2);\n" +
                        "commit\n" +
                        "begin\n" +
                        "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 2 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;\n" +
                        "commit\n" +
                        "begin\n" +
                        "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 2 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;\n" +
                        "commit\n" +
                        "begin\n" +
                        "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;\n" +
                        "commit\n", output);

    }

    @Test // (timeout = 3000)
    public void textExport100k() throws Exception {
        createData(50_000, db);
        Writer writer = new BufferedWriter(new FileWriter("target/export.cql"));
        long time = System.currentTimeMillis();
        writer.write(doOutput(db, 1000));
        long timeTaken = System.currentTimeMillis() - time;
        System.out.println("time = " + timeTaken + " ms");
        // takes 1.3s to generate
        writer.close();
        assertTrue("time more than 3s", timeTaken < 3000);
    }

    private String doOutput(GraphDatabaseService db, int batchSize) throws IOException {
        StringWriter writer = new StringWriter();
        try (Transaction tx = db.beginTx()) {
            DatabaseSubGraph subGraph = new DatabaseSubGraph(db);
            MultiStatementCypherSubGraphExporter exporter = new MultiStatementCypherSubGraphExporter(subGraph);
            exporter.export(new PrintWriter(writer), batchSize,new ProgressReporter(null,null));
            writer.close();
            tx.success();
        }
        return writer.toString();
    }
}
