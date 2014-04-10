package org.neo4j.shell.tools.imp.format;

import org.neo4j.cypher.export.SubGraph;
import org.neo4j.shell.tools.imp.util.Config;
import org.neo4j.shell.tools.imp.util.ElementCounter;
import org.neo4j.shell.tools.imp.util.Reporter;

import java.io.Reader;
import java.io.Writer;

/**
 * @author mh
 * @since 17.01.14
 */
public interface Format {
    ElementCounter load(Reader reader, Reporter reporter, Config config) throws Exception;
    ElementCounter dump(SubGraph graph, Writer writer, Reporter reporter, Config config) throws Exception;
}
