package org.neo4j.shell.tools.imp.util;

import org.neo4j.shell.Output;
import org.neo4j.shell.OutputAsWriter;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.util.Objects;

/**
 * Created by mh on 12.07.13.
 */
public class FileUtils {

    public static final int MEGABYTE = 1024 * 1024;

    public static CountingReader readerFor(String fileName) throws IOException {
        if (fileName==null) return null;
        if (fileName.startsWith("http") || fileName.startsWith("file:")) {
            URL url = new URL(fileName);
            URLConnection conn = url.openConnection();
            long size = conn.getContentLengthLong();
            Reader reader = new InputStreamReader(url.openStream(),"UTF-8");
            return new CountingReader(reader,size);
        }
        File file = new File(fileName);
        if (!file.exists() || !file.isFile() || !file.canRead()) throw new IOException("Cannot open file "+fileName+" for reading.");
        return new CountingReader(file);
    }

    public static PrintWriter getPrintWriter(String fileName, Output out) throws IOException {
        if (fileName == null) return null;
        Writer writer = fileName.equals("-") ? new OutputAsWriter(out) : new BufferedWriter(new FileWriter(fileName),MEGABYTE);
        return new PrintWriter(writer);
    }

}
