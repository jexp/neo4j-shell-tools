package org.neo4j.shell.tools.imp.util;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

/**
* @author mh
* @since 16.01.14
*/
public class BatchTransaction implements AutoCloseable {
    GraphDatabaseService gdb;
    int batchSize;
    private Reporter reporter;
    Transaction tx;
    int count = 0;
    int batchCount = 0;

    public BatchTransaction(GraphDatabaseService gdb, int batchSize, Reporter reporter) {
        this.gdb = gdb;
        this.batchSize = batchSize;
        this.reporter = reporter;
        tx = gdb.beginTx();
    }

    public void increment() {
        count++;batchCount++;
        if (batchCount >= batchSize) {
            doCommit(true);
        }
    }

    public void commit() {
        doCommit(true);
    }

    private void doCommit(boolean log) {
        tx.success();
        tx.close();
        if (log && reporter!=null) reporter.progress("commit after " + count + " row(s) ");
        tx = gdb.beginTx();
        batchCount = 0;
    }

    @Override
    public void close() {
        if (tx!=null) {
            tx.success();
            tx.close();
            if (reporter!=null) reporter.progress("finish after " + count + " row(s) ");
        }
    }
}
