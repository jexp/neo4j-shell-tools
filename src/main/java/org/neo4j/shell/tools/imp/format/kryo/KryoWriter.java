package org.neo4j.shell.tools.imp.format.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.shell.tools.imp.util.Config;
import org.neo4j.shell.tools.imp.util.Reporter;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by efulton on 5/5/16.
 */
public class KryoWriter {

    private Kryo kryo;
    public KryoWriter() {
        this.kryo = new Kryo();
    }

    public void write(SubGraph graph, Output output, Reporter reporter, Config config) throws IOException {
        for (ConstraintDefinition constraintDefinition : graph.getConstraints()) {
            writeConstraint(output, constraintDefinition);
        }
        for (IndexDefinition indexDefinition : graph.getIndexes()) {
            writeIndex(output, indexDefinition);
        }
        for (Node node : graph.getNodes()) {
            int props = writeNode(output, node);
            reporter.update(1, 0, props);
        }
        for (Relationship rel : graph.getRelationships()) {
            int props = writeRelationship(output, rel);
            reporter.update(0, 1, props);
        }
        // WRITE END DUMP
        kryo.writeObject(output, KryoSerializationTypes.DUMP_END.name());
    }

    private void writeConstraint(Output output, ConstraintDefinition constraintDefinition) {
        // WRITE CONSTRAINT START
        kryo.writeObject(output, KryoSerializationTypes.CONSTRAINT.name());
        // WRITE CONSTRAINT LABEL
        kryo.writeObject(output, constraintDefinition.getLabel().name());
        // WRITE PROPERTY KEYS
        List<String> propertyKeys = new LinkedList<>();
        Iterator<String> propertyKeyIterator = constraintDefinition.getPropertyKeys().iterator();
        while (propertyKeyIterator.hasNext()) {
            propertyKeys.add(propertyKeyIterator.next());
        }
        kryo.writeObject(output, propertyKeys);
    }

    private void writeIndex(Output output, IndexDefinition indexDefinition) {
        // Constraints indices are handled when we write out constraints
        if (!indexDefinition.isConstraintIndex()) {
            // WRITE INDEX START
            kryo.writeObject(output, KryoSerializationTypes.INDEX.name());
            // WRITE INDEX LABEL
            kryo.writeObject(output, indexDefinition.getLabel().name());
            // WRITE PROPERTY KEYS
            List<String> propertyKeys = new LinkedList<>();
            Iterator<String> propertyKeyIterator = indexDefinition.getPropertyKeys().iterator();
            while (propertyKeyIterator.hasNext()) {
                propertyKeys.add(propertyKeyIterator.next());
            }
            kryo.writeObject(output, propertyKeys);
        }
    }

    private int writeNode(Output output, Node node) {
        // WRITE NODE START
        kryo.writeObject(output, KryoSerializationTypes.NODE.name());
        // WRITE ID
        kryo.writeObject(output, node.getId());
        // WRITE LABELS
        List<String> labels = new LinkedList<>();
        Iterator<Label> labelIterator = node.getLabels().iterator();
        while (labelIterator.hasNext()) {
            labels.add(labelIterator.next().name());
        }
        kryo.writeObject(output, labels);
        // WRITE PROPERTIES
        Map<String, Object> properties = node.getAllProperties();
        kryo.writeObject(output, node.getAllProperties());
        return properties.size();
    }

    private int writeRelationship(Output output, Relationship relationship) {
        // WRITE RELATIONSHIP
        kryo.writeObject(output, KryoSerializationTypes.RELATIONSHIP.name());
        kryo.writeObject(output, relationship.getId());
        // WRITE SOURCE
        kryo.writeObject(output, relationship.getStartNode().getId());
        // WRITE TARGET
        kryo.writeObject(output, relationship.getEndNode().getId());
        // WRITE LABEL
        kryo.writeObject(output, relationship.getType().name());
        // WRITE PROPERTIES
        Map<String, Object> properties = relationship.getAllProperties();
        kryo.writeObject(output, relationship.getAllProperties());
        return properties.size();
    }
}