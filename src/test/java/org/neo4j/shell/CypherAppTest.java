package org.neo4j.shell;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.Serializable;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.MapUtil.genericMap;
import static org.neo4j.helpers.collection.MapUtil.map;

/**
 * @author mh
 * @since 17.01.14
 */
@Ignore
public class CypherAppTest extends AbstractShellTest
{
    @Test
    public void cypherImportPerformanceTest() throws Exception
    {
        Map<String, Serializable> variables = genericMap( "PS1", "","quiet",true );
        ShellClient client = newShellClient( shellServer, variables );
        SilentLocalOutput out = new SilentLocalOutput();
        client.evaluate("begin");
        create(client, out, 0);
        long time = System.currentTimeMillis();
        for (int i=1;i<1_000_000;i++) {
            create(client, out, i);
            if (i % 50_000 == 0) {
                client.evaluate("commit");
                System.out.println("commit "+i+" "+(System.currentTimeMillis()-time)+" ms since start");
                client.evaluate("begin");
            }
        }
        client.evaluate("commit");
        time = System.currentTimeMillis() - time;
        System.out.println("time = " + time);
        assertTrue(time < 10000);
    }

    private void create(ShellClient client, Output out, int i) throws ShellException {
        String command = "create (n {name:'User" + i + "',age:" + i + "});";
        client.evaluate(command, out);
    }

    @Test
    public void testExecutionEngine() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ExecutionEngine engine = new ExecutionEngine(db);
        long time = System.currentTimeMillis();
        Map<String, Object> params = map("i", null);
        Map.Entry<String, Object> entry = params.entrySet().iterator().next();
        for (int i=1;i<1_000_000;i++) {
            entry.setValue(i);
            engine.execute("return {i}",params);
            if (i % 50_000 == 0) {
                System.out.println("commit " + i + " " + (System.currentTimeMillis() - time) + " ms since start");
            }
        }
        time = System.currentTimeMillis() - time;
        System.out.println("time = " + time);
    }
}
