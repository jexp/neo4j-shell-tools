package org.neo4j.shell.tools.imp;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.shell.tools.imp.format.graphml.XmlGraphMLReader;
import org.neo4j.shell.tools.imp.util.MapNodeCache;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by mh on 10.07.13.
 */
public class GraphMLReaderTest {

    private static String SIMPLE_GRAPHML = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\"  \n" +
            "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
            "    xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns\n" +
            "     http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\">\n" +
            "  <graph id=\"G\" edgedefault=\"undirected\">\n" +
            "    <node id=\"n0\"/>\n" +
            "    <node id=\"n1\"/>\n" +
            "    <node id=\"n2\"/>\n" +
            "    <edge source=\"n0\" target=\"n2\"/>\n" +
            "    <edge source=\"n1\" target=\"n2\"/>\n" +
            "  </graph>\n" +
            "</graphml>";
    private XmlGraphMLReader graphMLReader;
    private GraphDatabaseService gdb;

    @Before
    public void setUp() throws Exception {
        gdb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        graphMLReader = new XmlGraphMLReader(gdb).storeNodeIds();
    }

    @Test
    public void testReadSimpleFile() throws Exception {
        graphMLReader.parseXML(new StringReader(SIMPLE_GRAPHML), MapNodeCache.<String, Long>usingHashMap());
        try (Transaction tx = gdb.beginTx()) {
            assertNode(gdb.getNodeById(0), "n0");
            assertNode(gdb.getNodeById(1), "n1");
            assertNode(gdb.getNodeById(2), "n2");
            assertRel(0, "n0", "n2");
            assertRel(1, "n1", "n2");
            tx.success();
        }
    }

    @Test
    public void testReadFileWithAttributes() throws Exception {
        InputStream stream = getClass().getResourceAsStream("/graphml-with-attributes.xml");
        try {
            graphMLReader.parseXML(new InputStreamReader(stream),MapNodeCache.<String, Long>usingHashMap());
            try (Transaction tx = gdb.beginTx()) {
                assertNode(gdb.getNodeById(0), "n0", "green");
                assertNode(gdb.getNodeById(1), "n1", "yellow");
                assertNode(gdb.getNodeById(2), "n2","blue");
                assertNode(gdb.getNodeById(3), "n3", "red");
                assertNode(gdb.getNodeById(4), "n4", "yellow");
                assertRel(0, "n0", "n2", 1.0);
                assertRel(1, "n0", "n1", 1.0);
                assertRel(2,"n1","n3",2.0);
                assertRel(3,"n3","n2",-1);
                assertRel(6,"n5","n4",1.1);
                tx.success();
            }
        } finally {
            if (stream!=null) stream.close();
        }
    }

    private void assertNode(Node node, String id) {
        assertEquals("id",id, node.getProperty("id"));
    }
    private void assertNode(Node node, String id, String color) {
        assertNode(node,id);
        assertEquals("color",color, node.getProperty("color"));
    }
    private Relationship assertRel(int relId, String id1, String id2) {
        Relationship rel = gdb.getRelationshipById(relId);
        assertNode(rel.getStartNode(), id1);
        assertNode(rel.getEndNode(), id2);
        return rel;
    }

    private void assertRel(int relId, String id1, String id2, double weight) {
        Relationship relationship = assertRel(relId, id1, id2);
        assertEquals("weight", weight, relationship.getProperty("weight",-1.0));
    }

}
