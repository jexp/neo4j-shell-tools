package org.neo4j.shell.tools.imp.format;

import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.*;
import org.neo4j.shell.tools.imp.util.Config;
import org.neo4j.shell.tools.imp.util.ElementCounter;
import org.neo4j.shell.tools.imp.util.Reporter;
import org.neo4j.shell.tools.imp.util.WriterOutputStream;
import org.neo4j.visualization.SubgraphMapper;
import org.neo4j.visualization.graphviz.GraphvizWriter;

import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;

/**
 * @author mh
 * @since 17.01.14
 */
public class GraphVizFormat implements Format {
    private final GraphDatabaseService db;

    public GraphVizFormat(GraphDatabaseService db) {
        this.db = db;
    }

    @Override
    public ElementCounter load(Reader reader, Reporter reporter, Config config) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public ElementCounter dump(SubGraph graph, Writer writer, Reporter reporter, Config config) throws Exception {
        try (Transaction tx = db.beginTx()) {
            GraphvizWriter graphvizWriter = new GraphvizWriter();
            OutputStream os = new WriterOutputStream(writer);
            graphvizWriter.emit(os, new SubgraphMappingWalkerWrapper(graph));
            tx.success();
        }
        return reporter.getTotal();
    }

    private static class SubgraphMappingWalkerWrapper extends SubgraphMapper.SubgraphMappingWalker {
        private final SubGraph subGraph;

        private SubgraphMappingWalkerWrapper(SubGraph subGraph) {
            super(null);
            this.subGraph = subGraph;
        }

        @Override
        protected Iterable<Node> nodes() {
            return subGraph.getNodes();
        }

        @Override
        protected Iterable<Relationship> relationships() {
            return subGraph.getRelationships();
        }
    }
}
