package org.neo4j.shell.tools.imp.util;

import org.neo4j.graphdb.*;
import org.neo4j.shell.tools.imp.util.NodeCache;
import org.neo4j.shell.tools.imp.util.Reporter;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by mh on 10.07.13.
 */
public class GraphMLReader {

    private GraphDatabaseService gdb;
    private boolean storeNodeIds;
    private DynamicRelationshipType defaultRelType =DynamicRelationshipType.withName("UNKNOWN");
    private int batchSize = 40000;
    private Reporter reporter;

    public GraphMLReader storeNodeIds() {
        this.storeNodeIds = true;
        return this;
    }

    public GraphMLReader relType(String name) {
        this.defaultRelType = DynamicRelationshipType.withName(name);
        return this;
    }

    public GraphMLReader batchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }
    public GraphMLReader reporter(Reporter reporter) {
        this.reporter = reporter;
        return this;
    }

    enum Type {
        BOOLEAN() {
            Object parse(String value) {
                return Boolean.valueOf(value);
            }
        }, INT() {
            Object parse(String value) {
                return Integer.parseInt(value);
            }
        }, LONG() {
            Object parse(String value) {
                return Long.parseLong(value);
            }
        }, FLOAT() {
            Object parse(String value) {
                return Float.parseFloat(value);
            }
        }, DOUBLE() {
            Object parse(String value) {
                return Double.parseDouble(value);
            }

        }, STRING() {
            Object parse(String value) {
                return value;
            }
        };

        abstract Object parse(String value);

        public static Type forType(String type) {
            return valueOf(type.trim().toUpperCase());
        }
    }

    static class Key {
        String id;
        String name;
        boolean forNode;
        Type type;
        Object defaultValue;

        public Key(String id, String name, String type, String forNode) {
            this.id = id;
            this.name = name;
            this.type = Type.forType(type);
            this.forNode = forNode == null || forNode.equalsIgnoreCase("node");
        }

        public void setDefault(String data) {
            this.defaultValue = type.parse(data);
        }

        public Object parseValue(String input) {
            if (input == null || input.trim().isEmpty()) return defaultValue;
            return type.parse(input);
        }
    }

    public static final QName ID = QName.valueOf("id");
    public static final QName SOURCE = QName.valueOf("source");
    public static final QName TARGET = QName.valueOf("target");
    public static final QName LABEL = QName.valueOf("label");
    public static final QName FOR = QName.valueOf("for");
    public static final QName NAME = QName.valueOf("attr.name");
    public static final QName TYPE = QName.valueOf("attr.type");
    public static final QName KEY = QName.valueOf("key");

    public GraphMLReader(GraphDatabaseService gdb) {
        this.gdb = gdb;
    }

    public long parseXML(Reader input, NodeCache cache) throws XMLStreamException {
        XMLInputFactory inputFactory = XMLInputFactory.newInstance();
        XMLEventReader reader = inputFactory.createXMLEventReader(input);
        PropertyContainer last = null;
        Map<String, Key> nodeKeys = new HashMap<String, Key>();
        Map<String, Key> relKeys = new HashMap<String, Key>();
        long nodes=0;
        long rels=0;
        long properties=0;
        Transaction tx = gdb.beginTx();
        while (reader.hasNext()) {
            XMLEvent event = (XMLEvent) reader.next();
            if (event.isStartElement()) {
                if (((nodes+rels+properties) % (batchSize*10)) == 1) {
                    tx.success();tx.finish();
                    tx = gdb.beginTx();
                    if (reporter!=null) reporter.progress(nodes,rels,properties);
                }

                StartElement element = event.asStartElement();
                String name = element.getName().getLocalPart();

                if (name.equals("graphml") || name.equals("graph")) continue;
                if (name.equals("key")) {
                    String id = getAttribute(element, ID);
                    Key key = new Key(id, getAttribute(element, NAME), getAttribute(element, TYPE), getAttribute(element, FOR));

                    XMLEvent next = peek(reader);
                    if (next.isStartElement() && next.asStartElement().getName().getLocalPart().equals("default")) {
                        reader.nextEvent().asStartElement();
                        key.setDefault(reader.nextEvent().asCharacters().getData());
                    }
                    if (key.forNode) nodeKeys.put(id, key);
                    else relKeys.put(id, key);
                    continue;
                }
                if (name.equals("data")) {
                    if (last == null) continue;
                    String id = getAttribute(element, KEY);
                    Key key = last instanceof Node ? nodeKeys.get(id) : relKeys.get(id);
                    Object value = key.defaultValue;
                    XMLEvent next = peek(reader);
                    if (next.isCharacters()) {
                        value = key.parseValue(reader.nextEvent().asCharacters().getData());
                    }
                    if (value!=null) {
                        last.setProperty(key.name, value);
                        properties ++;
                    }
                    continue;
                }
                if (name.equals("node")) {
                    String id = getAttribute(element, ID);
                    Node node = gdb.createNode();
                    if (storeNodeIds) node.setProperty("id",id);
                    setDefaults(nodeKeys, node);
                    last = node;
                    cache.put(id, node.getId());
                    nodes++;
                    continue;
                }
                if (name.equals("edge")) {
                    String source = getAttribute(element, SOURCE);
                    String target = getAttribute(element, TARGET);
                    String label = getAttribute(element, LABEL);
                    Node from = gdb.getNodeById(cache.get(source));
                    Node to = gdb.getNodeById(cache.get(target));
                    RelationshipType type = label != null ? DynamicRelationshipType.withName(label) : defaultRelType;
                    Relationship relationship = from.createRelationshipTo(to, type);
                    setDefaults(relKeys,relationship);
                    rels++;
                    last = relationship;
                    continue;
                }
            }
        }
        tx.success();tx.finish();
        if (reporter!=null) reporter.finish(nodes, rels, properties);
        return nodes + rels;
    }

    private XMLEvent peek(XMLEventReader reader) throws XMLStreamException {
        XMLEvent peek = reader.peek();
        if (peek.isCharacters() && (peek.asCharacters().isWhiteSpace())) {
            reader.nextEvent();
            return peek(reader);
        }
        return peek;
    }

    private void setDefaults(Map<String, Key> keys, PropertyContainer pc) {
        if (keys.isEmpty()) return;
        for (Key key : keys.values()) {
            if (key.defaultValue!=null) pc.setProperty(key.name,key.defaultValue);
        }
    }

    private String getAttribute(StartElement element, QName qname) {
        Attribute attribute = element.getAttributeByName(qname);
        return attribute != null ? attribute.getValue() : null;
    }
}
