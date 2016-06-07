package org.neo4j.shell.tools.imp.format.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.shell.Output;
import org.neo4j.shell.tools.imp.util.BatchTransaction;
import org.neo4j.shell.tools.imp.util.NodeCache;
import org.neo4j.shell.tools.imp.util.ProgressReporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by efulton on 5/5/16.
 */
public class KryoReader {
    private final GraphDatabaseService gdb;
    private final DynamicRelationshipType relType;
    private final Kryo kryo;
    private final ProgressReporter reporter;
    private final Output output;

    public KryoReader(GraphDatabaseService gdb, String relType, ProgressReporter reporter, Output output) {
        this.gdb = gdb;
        this.relType = relType != null ? DynamicRelationshipType.withName(relType) : DynamicRelationshipType.withName("UNKNOWN");
        this.reporter = reporter;
        this.output = output;
        this.kryo = new Kryo();
    }

    public long readBinaryDump(Input input, BatchTransaction tx, NodeCache<Long, Long> cache) throws IOException {
        if (input.available() > 0) {
            KryoSerializationTypes type = KryoSerializationTypes.valueOf(kryo.readObject(input, String.class));

            // Cannot perform data updates in a transaction that has performed schema updates
            output.println("Importing Indices and Constraints");
            while (KryoSerializationTypes.CONSTRAINT.equals(type) || KryoSerializationTypes.INDEX.equals(type)) {
                if (KryoSerializationTypes.CONSTRAINT.equals(type)) {
                    Label label = DynamicLabel.label(kryo.readObject(input, String.class));
                    List<String> propertyKeys = kryo.readObject(input, ArrayList.class);

                    // Add Constraint to Database
                    ConstraintCreator constraintCreator = gdb.schema().constraintFor(label);
                    if (propertyKeys.size() == 1) {
                        constraintCreator = constraintCreator.assertPropertyIsUnique(propertyKeys.get(0));
                    }
                    constraintCreator.create();

                    // Increment Transaction for Constraint
                    tx.increment();
                } else if (KryoSerializationTypes.INDEX.equals(type)) {
                    Label label = DynamicLabel.label(kryo.readObject(input, String.class));
                    List<String> propertyKeys = kryo.readObject(input, ArrayList.class);

                    // Add Index to Database
                    if (propertyKeys.size() != 1) {
                        throw new RuntimeException("Index was malformed");
                    }
                    gdb.schema().indexFor(label).on(propertyKeys.get(0)).create();

                    // Increment Transaction for Index
                    tx.increment();
                } else {
                    throw new RuntimeException("Encountered corrupt serialization");
                }

                type = KryoSerializationTypes.valueOf(kryo.readObject(input, String.class));
            }

            // Write Transaction
            output.println("Index Import Complete");
            tx.manualCommit(false);

            //  Load up rest of database dump
            output.println("Importing Nodes and Edges");
            while (!KryoSerializationTypes.DUMP_END.equals(type)) {
                if (KryoSerializationTypes.NODE.equals(type)) {
                    // Read Node
                    long id = kryo.readObject(input, Long.class);
                    List<String> labelStrings = kryo.readObject(input, ArrayList.class);
                    List<Label> labels = new ArrayList<>(labelStrings.size());
                    for (String labelString : labelStrings) {
                        labels.add(DynamicLabel.label(labelString));
                    }
                    Map<String, Object> properties = kryo.readObject(input, HashMap.class);

                    // Add Node to Database
                    Node node = gdb.createNode(labels.toArray(new Label[labels.size()]));
                    if (properties != null) {
                        for (Map.Entry<String, Object> entry : properties.entrySet()) {
                            node.setProperty(entry.getKey(), entry.getValue());
                        }
                    }

                    // Cache Node Id
                    cache.put(id, node.getId());

                    // Increment Transaction for Node
                    tx.increment();
                    reporter.update(1, 0, properties.size());
                } else if (KryoSerializationTypes.RELATIONSHIP.equals(type)) {
                    // Read Relationship
                    long id = kryo.readObject(input, Long.class);
                    long startNodeId = kryo.readObject(input, Long.class);
                    long targetNodeId = kryo.readObject(input, Long.class);
                    String relationshipLabel = kryo.readObject(input, String.class);
                    Map<String, Object> properties = kryo.readObject(input, HashMap.class);

                    // Add Relationship to Database
                    RelationshipType relationshipType = relationshipLabel != null ?
                        DynamicRelationshipType.withName(relationshipLabel) : this.relType;
                    Node from = gdb.getNodeById(cache.get(startNodeId));
                    Node to = gdb.getNodeById(cache.get(targetNodeId));
                    Relationship relationship = from.createRelationshipTo(to, relationshipType);
                    if (properties != null) {
                        for (Map.Entry<String, Object> entry : properties.entrySet()) {
                            relationship.setProperty(entry.getKey(), entry.getValue());
                        }
                    }

                    // Increment Transaction for Edge
                    tx.increment();
                    reporter.update(0, 1, properties.size());
                } else {
                    throw new RuntimeException("Encountered corrupt serialization");
                }

                type = KryoSerializationTypes.valueOf(kryo.readObject(input, String.class));
            }
        }

        output.println("Import Complete");
        return tx.getCount();
    }
}