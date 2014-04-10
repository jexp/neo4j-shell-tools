package org.neo4j.shell.tools.imp.format;

import org.apache.commons.lang.StringUtils;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.shell.tools.imp.util.Config;
import org.neo4j.shell.tools.imp.util.ElementCounter;
import org.neo4j.shell.tools.imp.util.MetaInformation;
import org.neo4j.shell.tools.imp.util.Reporter;

import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.*;

import static org.neo4j.helpers.collection.Iterables.join;
import static org.neo4j.shell.tools.imp.util.MetaInformation.collectPropTypesForNodes;
import static org.neo4j.shell.tools.imp.util.MetaInformation.collectPropTypesForRelationships;
import static org.neo4j.shell.tools.imp.util.MetaInformation.getLabelsString;

/**
 * @author mh
 * @since 19.01.14
 * todo opencsv
 */
public class CsvFormat implements Format {
    private final GraphDatabaseService db;

    public CsvFormat(GraphDatabaseService db) {
        this.db = db;
    }

    @Override
    public ElementCounter load(Reader reader, Reporter reporter, Config config) throws Exception {
        return null;
    }

    @Override
    public ElementCounter dump(SubGraph graph, Writer writer, Reporter reporter, Config config) throws Exception {
        try (Transaction tx = db.beginTx()) {
            PrintWriter out = new PrintWriter(writer);
            writeNodes(graph, out, reporter,config);
            writeRels(graph, out, reporter,config);
            tx.success();
            return reporter.getTotal();
        }
    }

    private Collection<String> generateHeader(Map<String,Class> nodePropTypes, String...starters) {
        List<String> result = new ArrayList<String>();
        Collections.addAll(result, starters);
        for (Map.Entry<String, Class> entry : nodePropTypes.entrySet()) {
            String type = MetaInformation.typeFor(entry.getValue(), null);
            if (type==null || type.equals("string")) result.add(entry.getKey());
            else result.add(entry.getKey()+":"+ type);
        }
        return result;
    }

    private void writeNodes(SubGraph graph, PrintWriter out, Reporter reporter, Config config) {
        Map<String,Class> nodePropTypes = collectPropTypesForNodes(graph);
        String delim = config.getDelim();
        Collection<String> header = generateHeader(nodePropTypes, "id:id", "labels:label");
        out.println(join(header, delim)); // todo types
        Object[] row=new Object[header.size()];
        for (Node node : graph.getNodes()) {
            row[0]=node.getId();
            row[1]=getLabelsString(node);
            collectProps(nodePropTypes.keySet(), node, reporter, row, 2);
            out.println(StringUtils.join(row, delim));
            reporter.update(1, 0, 0);
        }
    }

    private void collectProps(Collection<String> fields, PropertyContainer pc, Reporter reporter, Object[] row, int offset) {
        for (String field : fields) {
            if (pc.hasProperty(field)) {
                row[offset] = pc.getProperty(field);
                reporter.update(0,0,1);
            }
            else {
                row[offset] = "";
            }
            offset++;
        }
    }

    private String join(Collection<String> elements, String delim) {
        return Iterables.join(delim, elements);
    }

    private void writeRels(SubGraph graph, PrintWriter out, Reporter reporter, Config config) {
        Map<String,Class> nodePropTypes = collectPropTypesForRelationships(graph);
        String delim = config.getDelim();
        Collection<String> header = generateHeader(nodePropTypes, "start:id", "end:id", "type:label");
        out.println(join(header, delim)); // todo types
        Object[] row=new Object[header.size()];
        for (Relationship rel : graph.getRelationships()) {
            row[0]=rel.getStartNode().getId();
            row[1]=rel.getEndNode().getId();
            row[2]=rel.getType().name();
            collectProps(nodePropTypes.keySet(), rel, reporter, row, 3);
            out.println(StringUtils.join(row,delim));
            reporter.update(0,1,0);
        }
    }
}
