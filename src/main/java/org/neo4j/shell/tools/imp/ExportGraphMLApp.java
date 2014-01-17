package org.neo4j.shell.tools.imp;

import org.neo4j.graphdb.*;
import org.neo4j.shell.*;
import org.neo4j.shell.impl.AbstractApp;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.shell.tools.imp.util.ProgressReporter;
import org.neo4j.shell.tools.imp.util.Reporter;
import org.neo4j.tooling.GlobalGraphOperations;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import static java.util.Arrays.asList;
import static org.neo4j.helpers.collection.Iterables.join;

/**
 * TODO: arrays, labels, rel-types, key-types
 * @author mh
 * @since 17.01.14
 */
public class ExportGraphMLApp extends AbstractApp {

    {
        addOptionDefinition( "o", new OptionDefinition( OptionValueType.MUST,
                "Output GraphML file" ) );
        addOptionDefinition( "t", new OptionDefinition( OptionValueType.MAY,
                "Write Key Types upfront (double pass)" ) );
    }

    @Override
    public String getName() {
        return "export-graphml";
    }


    @Override
    public GraphDatabaseShellServer getServer() {
        return (GraphDatabaseShellServer) super.getServer();
    }

    @Override
    public Continuation execute(AppCommandParser parser, Session session, Output out) throws Exception {
        String fileName = parser.option("o", null);
        BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
        boolean writeKeyTypes = parser.options().containsKey("t");
        ProgressReporter reporter = new ProgressReporter(null, out);
        writeData(reporter, fileName, writer,writeKeyTypes);
        writer.close();
        return Continuation.INPUT_COMPLETE;
    }

    private void writeData(Reporter reporter, String fileName, BufferedWriter writer, boolean writeKeyTypes) throws IOException {
        GraphDatabaseService db = getServer().getDb();
        try (Transaction tx = db.beginTx()) {
            writeHeader(writer);
            GlobalGraphOperations ops = GlobalGraphOperations.at(db);
            if (writeKeyTypes) writeKeyTypes(writer, ops);
            for (Node node : ops.getAllNodes()) {
                int props=writeNode(writer,node);
                reporter.update(1,0,props);
            }
            for (Relationship rel : ops.getAllRelationships()) {
                int props=writeRelationship(writer, rel);
                reporter.update(0,1,props);
            }
            writeFooter(writer);
            tx.success();
        }
        reporter.progress("Wrote to GraphML-file " + fileName);
    }

    private void writeKeyTypes(BufferedWriter writer, GlobalGraphOperations ops) throws IOException {
        Map<String,Class> keyTypes = new HashMap<>();
        for (Node node : ops.getAllNodes()) {
            updateKeyTypes(keyTypes, node);
        }
        writeKeyTypes(writer, keyTypes, "node");
        keyTypes.clear();
        for (Relationship rel : ops.getAllRelationships()) {
            updateKeyTypes(keyTypes, rel);
        }
        writeKeyTypes(writer, keyTypes, "edge");
    }

    private void writeKeyTypes(BufferedWriter writer, Map<String, Class> keyTypes, String forType) throws IOException {
        for (Map.Entry<String, Class> entry : keyTypes.entrySet()) {
            String type = typeFor(entry.getValue());
            if (type == null) continue;
            writer.write("<key id=\"" + entry.getKey() + "\" for=\"" + forType + "\" attr.name=\"" + entry.getKey() + "\" attr.type=\"" + type + "\"/>\n");
        }
    }

    Set<String> allowed = new HashSet<>(asList("boolean", "int", "long", "float", "double", "string"));
    private String typeFor(Class value) {
        if (value==void.class) return null;
        if (value.isArray()) return null; // TODO arrays
        String name = value.getSimpleName().toLowerCase();
        if (name.equals("integer")) return "int";
        if (allowed.contains(name)) return name;
        if (Number.class.isAssignableFrom(value)) return "int";
        return null;
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

    private int writeNode(BufferedWriter writer, Node node) throws IOException {
        writer.write("<node id=\"n"+node.getId()+"\"");
        writeLabels(writer, node);
        writer.write(">");
        writeLabelsAsData(writer,node);
        int props = writeProps(writer, node);
        writer.write("</node>\n");
        return props;
    }

    private void writeLabels(BufferedWriter writer, Node node) throws IOException {
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
    private void writeLabelsAsData(BufferedWriter writer, Node node) throws IOException {
        Iterator<Label> it = node.getLabels().iterator();
        if (it.hasNext()) {
            writeData(writer,"labels",join(",", it));
        }
    }

    private int writeRelationship(BufferedWriter writer, Relationship rel) throws IOException {
        writer.write("<edge id=\"e"+rel.getId()+"\" source=\"n"+rel.getStartNode().getId()+"\" target=\"n"+rel.getEndNode().getId()+"\" label=\""+rel.getType().name()+"\">");
        writeData(writer,"type",rel.getType().name());
        int props = writeProps(writer, rel);
        writer.write("</edge>\n");
        return props;
    }

    private int writeProps(BufferedWriter writer, PropertyContainer node) throws IOException {
        int count=0;
        for (String prop : node.getPropertyKeys()) {
            Object value = node.getProperty(prop);
            writeData(writer, prop, value);
            count++;
        }
        return count;
    }

    private void writeData(BufferedWriter writer, String prop, Object value) throws IOException {
        writer.write("<data key=\""+prop+"\">"+value+"</data>");
    }

    private void writeFooter(BufferedWriter writer) throws IOException {
        writer.write("</graph>\n" +
                "</graphml>");
    }

    private void writeHeader(BufferedWriter writer) throws IOException {
        writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\"\n" +
                " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
                " xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns\n" +
                " http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\">\n" +
                "<graph id=\"G\" edgedefault=\"directed\">\n");
    }
}
