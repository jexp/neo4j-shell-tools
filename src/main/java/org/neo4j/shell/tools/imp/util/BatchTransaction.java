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

    public BatchTransaction(GraphDatabaseService gdb, int batchSize, Reporter reporter) {
        this.gdb = gdb;
        this.batchSize = batchSize;
        this.reporter = reporter;
        tx = gdb.beginTx();
    }

    public void increment() {
        count++;
        if (count % batchSize == 0) {
            tx.success();
            tx.close();
            if (reporter!=null) reporter.progress("commit after " + count + " row(s) ");
            tx = gdb.beginTx();
        }
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
