package org.neo4j.shell.tools.imp.format;

import com.nigelsmall.geoff.Subgraph;
import com.nigelsmall.geoff.loader.NeoLoader;
import com.nigelsmall.geoff.reader.GeoffReader;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.shell.tools.imp.util.Config;
import org.neo4j.shell.tools.imp.util.ElementCounter;
import org.neo4j.shell.tools.imp.util.Reporter;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.Map;

/**
 * @author mh
 * @since 17.01.14
 */
public class GeoffFormat implements Format {
    private final GraphDatabaseService db;

    public GeoffFormat(GraphDatabaseService db) {
        this.db = db;
    }

    @Override
    public ElementCounter load(Reader reader, Reporter reporter, Config config) throws IOException {
        try (Transaction tx = db.beginTx())  {
            NeoLoader loader = new NeoLoader(db);
            Subgraph subgraph = new GeoffReader(reader).readSubgraph();
            Map<String, Node> result = loader.load(subgraph);
            tx.success();
            reporter.update(result.size(),0,0); // todo more insights
        }
        return reporter.getTotal();
    }

    @Override
    public ElementCounter dump(SubGraph graph, Writer writer, Reporter reporter, Config config) throws IOException {
        try (Transaction tx = db.beginTx())  {
            GeoffExportService exportService = new GeoffExportService(graph);
            exportService.export(writer,reporter);
            tx.success();
            return reporter.getTotal();
        }
    }
}
