package org.neo4j.shell.tools.imp.format;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.StringWriter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.shell.tools.imp.util.Config.config;

/**
 * @author mh
 * @since 19.01.14
 */
public class CsvFormatTest {

    public static final DynamicRelationshipType KNOWS = DynamicRelationshipType.withName("KNOWS");

    @Test
    @Ignore
    public void testLoad() throws Exception {
        fail("not implemented");

    }

    @Test
    public void testDump() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        createData(db);
        CsvFormat format = new CsvFormat(db);
        StringWriter writer = new StringWriter();
        format.dump(new DatabaseSubGraph(db), writer,new TestReporter(), config());
        String output = writer.toString();
        System.out.println("output = " + output);
        assertEquals("id:id,labels:label,foo,age:int\n" +
                     "0,,bar,\n" +
                     "1,:User,,28\n" +
                     "start:id,end:id,type:label,counter:int\n" +
                     "0,1,KNOWS,1\n" +
                     "1,0,KNOWS,2\n", output);
    }

    private void createData(GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            Node n1 = db.createNode();
            n1.setProperty("foo","bar");
            Node n2 = db.createNode(DynamicLabel.label("User"));
            n2.setProperty("age",28);
            Relationship rel1 = n1.createRelationshipTo(n2, KNOWS);
            rel1.setProperty("counter",1);
            Relationship rel2 = n2.createRelationshipTo(n1, KNOWS);
            rel2.setProperty("counter", 2);
            tx.success();
        }
    }
}
