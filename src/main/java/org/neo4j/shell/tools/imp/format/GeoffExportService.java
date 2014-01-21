package org.neo4j.shell.tools.imp.format;

import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.shell.tools.imp.util.Json;
import org.neo4j.shell.tools.imp.util.Reporter;

import java.io.IOException;
import java.io.Writer;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author mh
 * @since 17.01.14
 */
class GeoffExportService {
    private final SubGraph gdb;

    GeoffExportService(SubGraph gdb) {
        this.gdb = gdb;
    }

    public void export(Writer writer, Reporter reporter) throws IOException {
        appendNodes(writer,reporter);
        appendRelationships(writer,reporter);
    }

    private void appendRelationships(Writer writer, Reporter reporter) throws IOException {
        for (Relationship rel : gdb.getRelationships()) {
            formatNodeId(writer, rel.getStartNode());
            writer.write("-[:");
            writer.write(rel.getType().name());
            reporter.update(0, 1, formatProperties(writer, rel));
            writer.write("]->");
            formatNodeId(writer, rel.getEndNode());
            writer.write("\n");
        }
    }

    private void appendRelationship(Writer writer, Relationship rel) throws IOException {
        formatNodeId(writer, rel.getStartNode());
        writer.write("-[:");
        writer.write(rel.getType().name());
        writer.write("]->");
        formatNodeId(writer, rel.getEndNode());
        formatProperties(writer, rel);
    }

    private void appendNodes(Writer writer, Reporter reporter) throws IOException {
        for (Node node : gdb.getNodes()) {
            writer.write("(");
            writer.write(Long.toString(node.getId()));
            reporter.update(1, 0, formatProperties(writer, node));
            writer.write(")\n");
        }
    }

    private void formatNodeId(Writer writer, Node n) throws IOException {
        writer.write("(");
        writer.write(Long.toString(n.getId()));
        writer.write(")");
    }

    private int formatProperties(Writer writer, PropertyContainer pc) throws IOException {
        final Map<String, Object> properties = toMap(pc);
        if (properties.isEmpty()) return 0;
        writer.write(" ");
        writer.write(Json.toJson(properties));
        return properties.size();
    }

    Map<String, Object> toMap(PropertyContainer pc) {
        Map<String, Object> result = new TreeMap<>();
        for (String prop : pc.getPropertyKeys()) {
            result.put(prop, pc.getProperty(prop));
        }
        return result;
    }

}