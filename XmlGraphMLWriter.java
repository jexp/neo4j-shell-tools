package org.neo4j.shell.tools.imp.format.graphml;

import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.*;
import org.neo4j.shell.tools.imp.util.Config;
import org.neo4j.shell.tools.imp.util.MetaInformation;
import org.neo4j.shell.tools.imp.util.Reporter;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Array;
import java.util.*;

import static org.neo4j.shell.tools.imp.util.MetaInformation.getLabelsString;

/**
 * @author mh
 * @since 21.01.14
 */
public class XmlGraphMLWriter {

	public void write(SubGraph graph, Writer writer, Reporter reporter, Config config) throws IOException, XMLStreamException {
		XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
		XMLStreamWriter xmlWriter = xmlOutputFactory.createXMLStreamWriter(writer);
		writeHeader(xmlWriter);
		if (config.useTypes()) writeKeyTypes(xmlWriter, graph);
		for (Node node : graph.getNodes()) {
			int props = writeNode(xmlWriter, node);
			reporter.update(1, 0, props);
		}
		for (Relationship rel : graph.getRelationships()) {
			int props = writeRelationship(xmlWriter, rel);
			reporter.update(0, 1, props);
		}
		writeFooter(xmlWriter);
	}

	private void writeKeyTypes(XMLStreamWriter writer, SubGraph ops) throws IOException, XMLStreamException {
		Map<String, Class> keyTypes = new HashMap<>();
		for (Node node : ops.getNodes()) {
			updateKeyTypes(keyTypes, node);
		}
		writeKeyTypes(writer, keyTypes, "node");
		keyTypes.clear();
		for (Relationship rel : ops.getRelationships()) {
			updateKeyTypes(keyTypes, rel);
		}
		writeKeyTypes(writer, keyTypes, "edge");
	}

	private void writeKeyTypes(XMLStreamWriter writer, Map<String, Class> keyTypes, String forType) throws IOException, XMLStreamException {
		for (Map.Entry<String, Class> entry : keyTypes.entrySet()) {

			String type = MetaInformation.typeFor(entry.getValue(), MetaInformation.GRAPHML_ALLOWED);

			if (type == null) continue;
			writer.writeEmptyElement("key");
			writer.writeAttribute("id", entry.getKey());
			writer.writeAttribute("for", forType);
			writer.writeAttribute("attr.name", entry.getKey());
			writer.writeAttribute("attr.type", type);
			newLine(writer);
		}
	}

	private void updateKeyTypes(Map<String, Class> keyTypes, PropertyContainer pc) {
		for (String prop : pc.getPropertyKeys()) {
			Object value = pc.getProperty(prop);
			Class storedClass = keyTypes.get(prop);

			if (storedClass == null) {
				keyTypes.put(prop, value.getClass());

				continue;
			}
			if (storedClass == void.class || storedClass.equals(value.getClass())) continue;
			keyTypes.put(prop, void.class);
		}
	}

	private int writeNode(XMLStreamWriter writer, Node node) throws IOException, XMLStreamException {
		writer.writeStartElement("node");
		writer.writeAttribute("id", id(node));
		writeLabels(writer, node);
		writeLabelsAsData(writer, node);
		int props = writeProps(writer, node);
		endElement(writer);
		return props;
	}

	private String id(Node node) {
		return "n" + node.getId();
	}

	private void writeLabels(XMLStreamWriter writer, Node node) throws IOException, XMLStreamException {
		String labelsString = getLabelsString(node);
		if (!labelsString.isEmpty()) writer.writeAttribute("labels", labelsString);
	}

	private void writeLabelsAsData(XMLStreamWriter writer, Node node) throws IOException, XMLStreamException {
		String labelsString = getLabelsString(node);
		if (labelsString.isEmpty()) return;
		writeData(writer, "labels", labelsString);
	}

	private int writeRelationship(XMLStreamWriter writer, Relationship rel) throws IOException, XMLStreamException {
		writer.writeStartElement("edge");
		writer.writeAttribute("id", id(rel));
		writer.writeAttribute("source", id(rel.getStartNode()));
		writer.writeAttribute("target", id(rel.getEndNode()));
		writer.writeAttribute("label", rel.getType().name());
		writeData(writer, "label", rel.getType().name());
		int props = writeProps(writer, rel);
		endElement(writer);
		return props;
	}

	private String id(Relationship rel) {
		return "e" + rel.getId();
	}

	private void endElement(XMLStreamWriter writer) throws XMLStreamException {
		writer.writeEndElement();
		newLine(writer);
	}

	// modified by gquercini to add array support
	private int writeProps(XMLStreamWriter writer, PropertyContainer node) throws IOException, XMLStreamException {
		int count = 0;
		for (String prop : node.getPropertyKeys()) {
			Object value = node.getProperty(prop);
			Class valueClass = value.getClass();

			if (valueClass.isArray()) 
				writeArrayData(writer, prop, value); // added by gquercini to add array support
			else
				writeData(writer, prop, value);
			count++;
		}
		return count;
	}

	// added by gquercini to add array support
	private void writeArrayData(XMLStreamWriter writer, String prop, Object value) throws IOException, XMLStreamException {

		if ( value == null ) return;

		int length = Array.getLength(value);
		if (length == 0) return;
		
		ArrayList<String> arrayList = new ArrayList<String>();
		
		for(int i = 0; i < length; i += 1)
		{
			String v = Array.get(value, i).toString().trim();
			if ( v.length() == 0 ) continue;
			
			arrayList.add(v);
		}
		
		if ( arrayList.size() == 0 ) return;
		
		writer.writeStartElement("array-data");
		writer.writeAttribute("key", prop);

		for ( String s: arrayList ) {
			
			writer.writeStartElement("item");
			writer.writeCharacters(s);
			writer.writeEndElement();
		}

		writer.writeEndElement();
	}

	// modified by gquercini: if value is null or empty string, the property is not exported.
	private void writeData(XMLStreamWriter writer, String prop, Object value) throws IOException, XMLStreamException {

		if (value == null) return;

		String valueString = value.toString().trim();
		if ( valueString.length() == 0 ) return;

		writer.writeStartElement("data");
		writer.writeAttribute("key", prop);
		writer.writeCharacters(valueString);
		writer.writeEndElement();
	}

	private void writeFooter(XMLStreamWriter writer) throws IOException, XMLStreamException {
		endElement(writer);
		endElement(writer);
		writer.writeEndDocument();
	}

	private void writeHeader(XMLStreamWriter writer) throws IOException, XMLStreamException {
		writer.writeStartDocument("UTF-8", "1.0");
		newLine(writer);
		writer.writeStartElement("graphml"); // todo properties
		writer.writeNamespace("xmlns", "http://graphml.graphdrawing.org/xmlns");
		writer.writeAttribute("xmlns", "http://graphml.graphdrawing.org/xmlns", "xsi", "http://www.w3.org/2001/XMLSchema-instance");
		writer.writeAttribute("xsi", "", "schemaLocation", "http://graphml.graphdrawing.org/xmlns http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd");
		newLine(writer);
		writer.writeStartElement("graph");
		writer.writeAttribute("id", "G");
		writer.writeAttribute("edgedefault", "directed");
		newLine(writer);
	}

	private void newLine(XMLStreamWriter writer) throws XMLStreamException {
		writer.writeCharacters("\n");
	}
}
