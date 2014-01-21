package org.neo4j.shell.tools.imp.util;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

/**
 * @author mh
 * @since 17.01.14
 */
public class Json {
    final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static String toJson(Object value) {
        try {
            return OBJECT_MAPPER.writeValueAsString(value);
        } catch (IOException e) {
            throw new RuntimeException("Error creating JSON from "+value,e);
        }
    }

    public static void toJson(Object value, Writer writer) {
        try {
            OBJECT_MAPPER.writeValue(writer,value);
        } catch (IOException e) {
            throw new RuntimeException("Error creating JSON from "+value,e);
        }
    }

    public static Object fromJson(String value) {
        try {
            return OBJECT_MAPPER.readValue(value, Object.class);
        } catch (IOException e) {
            throw new RuntimeException("Error reading JSON from "+value,e);
        }
    }

    public static <T> T fromJson(String value,Class<T> type) {
        try {
            return OBJECT_MAPPER.readValue(value,type);
        } catch (IOException e) {
            throw new RuntimeException("Error reading JSON from "+value,e);
        }
    }
    public static <T> T fromJson(Reader reader,Class<T> type) {
        try {
            return OBJECT_MAPPER.readValue(reader, type);
        } catch (IOException e) {
            throw new RuntimeException("Error reading JSON from stream ",e);
        }
    }
}
