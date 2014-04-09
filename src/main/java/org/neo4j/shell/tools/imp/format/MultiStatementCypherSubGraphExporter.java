/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.shell.tools.imp.format;

import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.IteratorUtil;

import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.util.*;

/**
 * Idea is to lookup nodes for relationships via a unqiue index
 * either one inherent to the original node, or a artificial one that indexes the original node-id
 * and which is removed after the import.
 * <p/>
 * Outputs indexes and constraints at the beginning as their own transactions
 */
public class MultiStatementCypherSubGraphExporter {
    private final SubGraph graph;
    private final Map<String, String> uniqueConstraints;
    private final static String UNIQUE_ID_LABEL = " Import Node ";
    private final static String Q_UNIQUE_ID_LABEL = quote(UNIQUE_ID_LABEL);
    private final static String UNIQUE_ID_PROP = " import id ";
    private long artificialUniques = 0;

    public MultiStatementCypherSubGraphExporter(SubGraph graph) {
        this.graph = graph;
        uniqueConstraints = gatherUniqueConstraints();
    }

    public void export(PrintWriter out, int batchSize) {
        writeMetaInformation(out);
        begin(out);
        appendNodes(out, batchSize);
        appendRelationships(out, batchSize);
        commit(out);
        while (artificialUniques > 0) {
            begin(out);
            out.println("MATCH (n:"+Q_UNIQUE_ID_LABEL+") " +
                       " WITH n LIMIT "+batchSize+
                       " REMOVE n:"+Q_UNIQUE_ID_LABEL+" REMOVE n."+quote(UNIQUE_ID_PROP)+";");
            commit(out);
            artificialUniques -= batchSize;
        }
        begin(out);
        out.println(uniqueConstraint(UNIQUE_ID_LABEL,UNIQUE_ID_PROP).replaceAll("^CREATE","DROP")+";");
        commit(out);
    }

    private Map<String, String> gatherUniqueConstraints() {
        Map<String, String> result = new HashMap<>();
        for (IndexDefinition indexDefinition : graph.getIndexes()) {
            if (!indexDefinition.isConstraintIndex()) continue;
            result.put(indexDefinition.getLabel().name(), IteratorUtil.first(indexDefinition.getPropertyKeys()));
        }
        return result;
    }

    private void writeMetaInformation(PrintWriter out) {
        for (String index : exportIndexes()) {
            begin(out);
            out.println(index);
            commit(out);
        }
        begin(out);
        out.println(uniqueConstraint(UNIQUE_ID_LABEL, UNIQUE_ID_PROP));
        commit(out);
    }

    private void begin(PrintWriter out) {
        out.println("begin");
    }

    private void restart(PrintWriter out) {
        commit(out);
        begin(out);
    }

    private void commit(PrintWriter out) {
        out.println("commit");
    }

    private Collection<String> exportIndexes() {
        Collection<String> result = new ArrayList<>();
        for (IndexDefinition index : graph.getIndexes()) {
            String prop = IteratorUtil.single(index.getPropertyKeys());
            String label = index.getLabel().name();
            result.add(index.isConstraintIndex() ? uniqueConstraint(label, prop) : index(label, prop));
        }
        return result;
    }

    private String index(String label, String key) {
        return "CREATE INDEX ON :" + quote(label) + "(" + quote(key) + ");";
    }

    private String uniqueConstraint(String label, String key) {
        return "CREATE CONSTRAINT ON (node:" + quote(label) + ") ASSERT node." + quote(key) + " IS UNIQUE;";
    }

//    public Collection<String> exportConstraints() {
//        Collection<String> result=new ArrayList<String>();
//        for (ConstraintDefinition constraint : graph.constraints()) {
//            if (!constraint.isConstraintType(ConstraintType.UNIQUENESS)) continue;
//            StringBuilder keys=new StringBuilder();
//            for (String key : constraint.getPropertyKeys()) {
//                if (keys.length()>0) keys.append(", ");
//                keys.append(quote(key));
//            }
//            result.add("CREATE CONSTRAINT ON (node:" + quote(constraint.getLabel().name()) + ") ASSERT node." + keys + " IS UNIQUE;");
//        }
//        return result;
//    }

    public static String quote(String id) {
        return "`" + id + "`";
    }

    private boolean hasProperties(PropertyContainer node) {
        return node.getPropertyKeys().iterator().hasNext();
    }

    private String labelString(Node node) {
        Iterator<Label> labels = node.getLabels().iterator();
        StringBuilder result = new StringBuilder();
        boolean uniqueFound = false;
        while (labels.hasNext()) {
            Label next = labels.next();
            String labelName = next.name();
            if (uniqueConstraints.containsKey(labelName)) uniqueFound = true;
            result.append(":").append(quote(labelName));
        }
        if (!uniqueFound) {
            result.append(":").append(Q_UNIQUE_ID_LABEL);
            artificialUniques++;
        }
        return result.toString();
    }

    private long appendRelationships(PrintWriter out, int batchSize) {
        long count=0;
        for (Relationship rel : graph.getRelationships()) {
            if (++count % batchSize == 0) restart(out);
            appendRelationship(out, rel);
        }
        return count;
    }

    // match (n1),(n2) where id(n1) = 234 and id(n2) = 345 create (n1)-[:TYPE {props}]->(n2);
    // match (n1:` Import Node ` {` import id `:234}),(n2:` Import Node ` {` import id `:345}) create (n1)-[:TYPE {props}]->(n2);
    private void appendRelationship(PrintWriter out, Relationship rel) {
        out.print("MATCH ");
        out.print(nodeLookup("n1", rel.getStartNode()));
        out.print(", ");
        out.print(nodeLookup("n2", rel.getEndNode()));
        out.print(" CREATE (n1)-[:");
        out.print(quote(rel.getType().name()));
        formatProperties(out, rel, null);
        out.println("]->(n2);");
    }

    private String nodeLookup(String id, Node node) {
        for (Label l : node.getLabels()) {
            String label = l.name();
            String prop = uniqueConstraints.get(label);
            if (prop == null) continue;
            Object value = node.getProperty(prop);
            return nodeLookup(id, label, prop, value);
        }
        return nodeLookup(id, UNIQUE_ID_LABEL, UNIQUE_ID_PROP, node.getId());
    }

    private String nodeLookup(String id, String label, String prop, Object value) {
        return "(" + id + ":" + quote(label) + "{" + quote(prop) + ":" + toString(value) + "})";
    }

    private long appendNodes(PrintWriter out, int batchSize) {
        long count = 0;
        for (Node node : graph.getNodes()) {
            if (++count % batchSize == 0) restart(out);
            appendNode(out, node);
        }
        return count;
    }

    private void appendNode(PrintWriter out, Node node) {
        out.print("CREATE (");
        String labels = labelString(node);
        if (!labels.isEmpty()) {
            out.print(labels);
        }
        Long id = labels.endsWith(Q_UNIQUE_ID_LABEL) ? node.getId() : null;
        formatProperties(out, node, id);
        out.println(");");
    }

    private void formatProperties(PrintWriter out, PropertyContainer pc, Long id) {
        if (!hasProperties(pc) && id == null) return;
        out.print(" ");
        final String propertyString = formatProperties(pc, id);
        out.print(propertyString);
    }

    private String formatProperties(PropertyContainer pc, Long id) {
        StringBuilder result = new StringBuilder();
        if (id != null) {
            result.append(quote(UNIQUE_ID_PROP)).append(":").append(id);
        }
        List<String> keys = Iterables.toList(pc.getPropertyKeys());
        Collections.sort(keys);
        for (String prop : keys) {
            if (result.length() > 0) {
                result.append(", ");
            }
            result.append(quote(prop)).append(":");
            Object value = pc.getProperty(prop);
            result.append(toString(value));
        }
        return "{" + result + "}";
    }

    private String toString(Iterator<?> iterator) {
        StringBuilder result = new StringBuilder();
        while (iterator.hasNext()) {
            if (result.length() > 0) result.append(", ");
            Object value = iterator.next();
            result.append(toString(value));
        }
        return "[" + result + "]";
    }

    private String arrayToString(Object value) {
        StringBuilder result = new StringBuilder();
        int length = Array.getLength(value);
        for (int i = 0; i < length; i++) {
            if (i > 0) result.append(", ");
            result.append(toString(Array.get(value, i)));
        }
        return "[" + result + "]";
    }

    private String toString(Object value) {
        if (value == null) return "null";
        if (value instanceof String) return "\"" + value + "\"";
        if (value instanceof Iterator) {
            return toString(((Iterator) value));
        }
        if (value instanceof Iterable) {
            return toString(((Iterable) value).iterator());
        }
        if (value.getClass().isArray()) {
            return arrayToString(value);
        }
        return value.toString();
    }
}
