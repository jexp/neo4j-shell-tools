package org.neo4j.shell.tools.imp.format;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import static org.neo4j.graphdb.DynamicLabel.label;

/**
 * @author mh
 * @since 18.01.14
 */
public class MultiStatementCypherSubGraphExporterTest {

    public static final Label USER = label("User");
    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
    }

    @Test
    public void testExportSingleNodeGraph() throws Exception {
        createData(1, db);
        System.out.println(doOutput(db));
    }

    @Test
    public void testExportSingleRelationshipGraph() throws Exception {
        createData(2, db);
        System.out.println(doOutput(db));
    }

    @Test
    public void testExportSingleRelationshipGraphWithConstraint() throws Exception {
        try (Transaction tx = db.beginTx()) {
            db.schema().constraintFor(USER).assertPropertyIsUnique("name").create();
            tx.success();
        }
        createData(2, db);
        System.out.println(doOutput(db));
    }
    @Test
    @Ignore("Slow to create data")
    public void textExport100k() throws Exception {
        try (Transaction tx = db.beginTx()) {
            db.schema().constraintFor(USER).assertPropertyIsUnique("name").create();
            tx.success();
        }
        createData(50_000, db);
        FileWriter writer = new FileWriter("target/export.cql");
        long time = System.currentTimeMillis();
        writer.write(doOutput(db));
        System.out.println("time = " + (System.currentTimeMillis() - time) + " ms");
        // takes 1.3s to generate
        writer.close();
    }

    private String doOutput(GraphDatabaseService db) throws IOException {
        StringWriter writer = new StringWriter();
        try (Transaction tx = db.beginTx()) {
            DatabaseSubGraph subGraph = new DatabaseSubGraph(db);
            MultiStatementCypherSubGraphExporter exporter = new MultiStatementCypherSubGraphExporter(subGraph);
            exporter.export(new PrintWriter(writer),1000);
            writer.close();
            tx.success();
        }
        return writer.toString();
    }


    private void createData(int count, GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            Node n1 = db.createNode();
            Node n2;
            for (int i=1;i<count;i++) {
                n2 = i % 2 == 0 ? db.createNode() : db.createNode(USER);
                n2.setProperty("name","User"+i);
                Relationship rel = n1.createRelationshipTo(n2, DynamicRelationshipType.withName("KNOWS"));
                rel.setProperty("since",2010+i);
                n1 = n2;
            }
            tx.success();
        }
        System.out.println("Created Data");
    }
}
