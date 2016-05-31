package org.neo4j.shell.tools.imp;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.impl.CollectingOutput;
import org.neo4j.shell.impl.SameJvmClient;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

/**
 * Created by mh on 04.07.13.
 */
public class AutoIndexAppTest {

    private GraphDatabaseAPI db;
    private SameJvmClient client;
    private AutoIndexer<Node> nodeAutoIndexer;
    private AutoIndexer<Relationship> relAutoIndexer;

    @Test
    public void testAutoIndexNodes() throws Exception {
        assertEquals(false, nodeAutoIndexer.isEnabled());
        assertCommand("auto-index name", "Enabling auto-indexing of Node properties: [name]");
        assertEquals(true, nodeAutoIndexer.isEnabled());
        assertIndexProp(true, "name");
        assertCommand("auto-index -r name", "Disabling auto-indexing of Node properties: [name]");
        assertIndexProp(false, "name");
        assertCommand("auto-index foo bar", "Enabling auto-indexing of Node properties: [foo, bar]");
        assertIndexProp(true, "foo", "bar");
        assertCommand("auto-index -r foo bar", "Disabling auto-indexing of Node properties: [foo, bar]");
        assertIndexProp(false, "foo", "bar");
    }
    @Test
    public void testAutoIndexRels() throws Exception {
        assertEquals(false, relAutoIndexer.isEnabled());
        assertCommand("auto-index -t Relationship name", "Enabling auto-indexing of Relationship properties: [name]");
        assertEquals(true, relAutoIndexer.isEnabled());
        assertIndexRelProp(true, "name");
        assertCommand("auto-index -t Relationship -r name", "Disabling auto-indexing of Relationship properties: [name]");
        assertIndexRelProp(false, "name");
        assertCommand("auto-index -t Relationship foo bar", "Enabling auto-indexing of Relationship properties: [foo, bar]");
        assertIndexRelProp(true, "foo", "bar");
        assertCommand("auto-index -t Relationship -r foo bar", "Disabling auto-indexing of Relationship properties: [foo, bar]");
        assertIndexRelProp(false, "foo", "bar");
    }

    private void assertIndexProp(boolean expected, String...props) {
        for (String prop : props) {
            assertEquals(expected, nodeAutoIndexer.getAutoIndexedProperties().contains(prop));
        }
    }
    private void assertIndexRelProp(boolean expected, String...props) {
        for (String prop : props) {
            assertEquals(expected, relAutoIndexer.getAutoIndexedProperties().contains(prop));
        }
    }

    private void assertCommand(String command, String expected) throws RemoteException, ShellException {
        CollectingOutput out = new CollectingOutput();
        client.evaluate(command, out);
        assertEquals(expected,out.asString().trim());
    }

    @Before
    public void setUp() throws RemoteException, ShellException {
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
        client = new SameJvmClient(Collections.<String, Serializable>emptyMap(), new GraphDatabaseShellServer(db), new TestCtrlCHandler());
        nodeAutoIndexer = db.index().getNodeAutoIndexer();
        relAutoIndexer = db.index().getRelationshipAutoIndexer();
    }

}
