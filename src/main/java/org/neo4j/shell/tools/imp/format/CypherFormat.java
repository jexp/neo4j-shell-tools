package org.neo4j.shell.tools.imp.format;

import org.neo4j.cypher.export.SubGraph;
import org.neo4j.cypher.export.SubGraphExporter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.shell.tools.imp.util.*;

import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.HashMap;
import java.util.Scanner;
import java.util.regex.Pattern;

/**
 * @author mh
 * @since 17.01.14
 */
public class CypherFormat implements Format {
    public static final Pattern READ_FILE_PATTERN = Pattern.compile("(commit|begin|;)(\r?\n|$)", Pattern.CASE_INSENSITIVE);
    private final QueryParameterExtractor extractor = new QueryParameterExtractor();

    private static final int BATCH_SIZE = 20000;
    private final GraphDatabaseService db;

    public CypherFormat(GraphDatabaseService db) {
        this.db = db;
    }

    @Override
    public ElementCounter load(Reader reader, Reporter reporter, Config config) {
        Scanner scanner = new Scanner(reader).useDelimiter(READ_FILE_PATTERN);
        HashMap<String, Object> params = new HashMap<>();
        try (BatchTransaction tx = new BatchTransaction(db,BATCH_SIZE,reporter)) {
            while (scanner.hasNext()) {
                String query = scanner.next();
                if (query.trim().isEmpty()) { // begin or commit line
                    tx.commit();
                    continue;
                }
                params.clear();
                String queryWithParams = extractor.extract(query, params);
                Result result = db.execute(queryWithParams, params);
                ProgressReporter.update(result.getQueryStatistics(), reporter);
                tx.increment();
            }
        }
        return reporter.getTotal();
    }

    @Override
    public ElementCounter dump(SubGraph graph, Writer writer, Reporter reporter, Config config) {
        try (Transaction tx = db.beginTx()) {
            PrintWriter out = new PrintWriter(writer);
            new SubGraphExporter(graph).export(out);
            tx.success();
            return reporter.getTotal();
        }
    }
}
