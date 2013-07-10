package org.neo4j.shell.tools.imp.util;

import org.neo4j.shell.Output;

import java.rmi.RemoteException;

/**
* Created by mh on 10.07.13.
*/
public class ProgressReporter implements Reporter {
    private final SizeCounter sizeCounter;
    private Output out;
    long time;
    int counter;
    long start=System.currentTimeMillis();

    public ProgressReporter(SizeCounter sizeCounter, Output out) {
        this.sizeCounter = sizeCounter;
        this.out = out;
        this.time = start;
    }

    @Override
    public void progress(long nodes, long relationships, long properties) {
        long now = System.currentTimeMillis();
        println(String.format("%d. %d%%: nodes = %d rels = %d properties = %d time %d ms", counter++, percent(), nodes, relationships, properties, now - time));
        time = now;
    }

    private void println(String message) {
        try {
            out.println(message);
        } catch (RemoteException e) {
            throw new RuntimeException(e);
        }
    }

    private long percent() {
        return sizeCounter.getPercent();
    }

    @Override
    public void finish(long nodes, long relationships, long properties) {
        long now = System.currentTimeMillis();
        println(String.format("Finished: nodes = %d rels = %d properties = %d total time %d ms", nodes, relationships, properties, now - start));
    }

}
