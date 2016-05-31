package org.neo4j.shell.tools.imp.format.graphml;

import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.*;
import org.neo4j.shell.tools.imp.util.Config;
import org.neo4j.shell.tools.imp.util.FormatUtils;
import org.neo4j.shell.tools.imp.util.MetaInformation;
import org.neo4j.shell.tools.imp.util.Reporter;

import java.io.IOException;
import java.io.Writer;
import java.util.*;

import static org.neo4j.helpers.collection.Iterables.join;

/**
 * @author mh
 * @since 21.01.14
 */
public class SimpleGraphMLWriter {
    private final GraphDatabaseService db;

    public SimpleGraphMLWriter(GraphDatabaseService db) {
        this.db = db;
    }

    public void write(SubGraph graph, Writer writer, Reporter reporter, Config config) throws IOException {
        writeHeader(writer);
        if (config.useTypes()) writeKeyTypes(writer, graph);
        for (Node node : graph.getNodes()) {
            int props=writeNode(writer,node);
            reporter.update(1,0,props);
        }
        for (Relationship rel : graph.getRelationships()) {
            int props=writeRelationship(writer, rel);
            reporter.update(0,1,props);
        }
        writeFooter(writer);
    }
    private void writeKeyTypes(Writer writer, SubGraph graph) throws IOException {
        Map<String,Class> keyTypes = new HashMap<>();
        for (Node node : graph.getNodes()) {
            updateKeyTypes(keyTypes, node);
        }
        writeKeyTypes(writer, keyTypes, "node");
        keyTypes.clear();
        for (Relationship rel : graph.getRelationships()) {
            updateKeyTypes(keyTypes, rel);
        }
        writeKeyTypes(writer, keyTypes, "edge");
    }

    private void writeKeyTypes(Writer writer, Map<String, Class> keyTypes, String forType) throws IOException {
        for (Map.Entry<String, Class> entry : keyTypes.entrySet()) {
            String type = MetaInformation.typeFor(entry.getValue(), MetaInformation.GRAPHML_ALLOWED);
            if (type == null) continue;
            writer.write("<key id=\"" + entry.getKey() + "\" for=\"" + forType + "\" attr.name=\"" + entry.getKey() + "\" attr.type=\"" + type + "\"/>\n");
        }
    }

    private void updateKeyTypes(Map<String, Class> keyTypes, PropertyContainer pc) {
        for (String prop : pc.getPropertyKeys()) {
            Object value = pc.getProperty(prop);
            Class storedClass = keyTypes.get(prop);
            if (storedClass==null) {
                keyTypes.put(prop,value.getClass());
                continue;
            }
            if (storedClass == void.class || storedClass.equals(value.getClass())) continue;
            keyTypes.put(prop, void.class);
        }
    }

    private int writeNode(Writer writer, Node node) throws IOException {
        writer.write("<node id=\"n"+node.getId()+"\"");
        writeLabels(writer, node);
        writer.write(">");
        writeLabelsAsData(writer,node);
        int props = writeProps(writer, node);
        writer.write("</node>\n");
        return props;
    }

    private void writeLabels(Writer writer, Node node) throws IOException {
        Iterator<Label> it = node.getLabels().iterator();
        if (it.hasNext()) {
            writer.write(" labels=\"");
            while (it.hasNext()) {
                writer.write(it.next().name());
                if (it.hasNext()) writer.write(",");
            }
            writer.write("\" ");
        }
    }
    private void writeLabelsAsData(Writer writer, Node node) throws IOException {
        Iterator<Label> it = node.getLabels().iterator();
        if (it.hasNext()) {
            writeData(writer,"labels", FormatUtils.joinLabels(node,","));
        }
    }

    private int writeRelationship(Writer writer, Relationship rel) throws IOException {
        writer.write("<edge id=\"e"+rel.getId()+"\" source=\"n"+rel.getStartNode().getId()+"\" target=\"n"+rel.getEndNode().getId()+"\" label=\""+rel.getType().name()+"\">");
        writeData(writer,"label",rel.getType().name());
        int props = writeProps(writer, rel);
        writer.write("</edge>\n");
        return props;
    }

    private int writeProps(Writer writer, PropertyContainer node) throws IOException {
        int count=0;
        for (String prop : node.getPropertyKeys()) {
            Object value = node.getProperty(prop);
            writeData(writer, prop, value);
            count++;
        }
        return count;
    }

    private void writeData(Writer writer, String prop, Object value) throws IOException {
        writer.write("<data key=\""+prop+"\">"+value+"</data>");
    }

    private void writeFooter(Writer writer) throws IOException {
        writer.write("</graph>\n" +
                "</graphml>");
    }

    private void writeHeader(Writer writer) throws IOException {
        writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\"\n" +
                " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
                " xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns\n" +
                " http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\">\n" +
                "<graph id=\"G\" edgedefault=\"directed\">\n");
    }}
