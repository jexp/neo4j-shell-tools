package org.neo4j.shell.tools;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.shell.ShellClient;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.impl.CollectingOutput;

import java.rmi.RemoteException;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;

/**
 * Created by mh on 12.07.13.
 */
public class Asserts {
    public static void assertCommand(ShellClient client, String command, String... expected) throws RemoteException, ShellException {
        CollectingOutput out = new CollectingOutput();
        client.evaluate(command, out);
        assertEquals("command: "+ Iterables.join("\n",out),expected.length, Iterators.count(out.iterator()));
        Iterator<String> it = out.iterator();
        for (String s : expected) {
            String output = it.next().trim();
            if (s==null || s.isEmpty()) continue;
            assertEquals(output+" should contain "+s,true,output.contains(s));
        }
    }
}
