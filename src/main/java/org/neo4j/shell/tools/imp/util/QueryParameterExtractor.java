package org.neo4j.shell.tools.imp.util;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author mh
 * @since 17.01.14
 */
public class QueryParameterExtractor {

    public static final Pattern PATTERN = Pattern.compile("([=:]\\s*)([0-9.Ee+-]+|'[^\\\\']*'|\"[^\\\\\"]*\"|NULL|null)", Pattern.MULTILINE);

    public String extract(String query, Map<String, Object> parameters) {
        Matcher matcher = PATTERN.matcher(query);
        if (!matcher.find()) return query;
        StringBuffer sb = new StringBuffer();
        int counter = 0;
        do {
            String param = param(counter++);
            String value = matcher.group(2);
            parameters.put(param, convertValue(value));
            matcher.appendReplacement(sb, matcher.group(1) + "{`" + param + "`}");
        } while (matcher.find());
        matcher.appendTail(sb);
        return sb.toString();
    }

    private Object convertValue(String value) {
        if (value.equals("NULL") || value.equals("null")) return null;
        if (value.charAt(0)=='"' || value.charAt(0)=='\'') {
            return value.substring(1, value.length() - 1);
        } else {
            if (value.indexOf('.') != -1) {
                double d = Double.parseDouble(value);
                if (d < Float.MAX_VALUE) {
                    return (float)d;
                }
            }
            long l = Long.parseLong(value);
            if (l <= Byte.MAX_VALUE) return (byte)l;
            if (l <= Short.MAX_VALUE) return (short)l;
            if (l <= Integer.MAX_VALUE) return (int)l;
            return l;
        }
    }

    private String param(int i) {
        return " p "+i;
    }
}
