package org.neo4j.shell.tools.imp.util;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedOutputStream;
import java.io.Writer;
import java.nio.charset.Charset;

/**
 * @author mh
 * @since 17.01.14
 */
public class WriterOutputStream extends OutputStream {
    private final Writer writer;


    public WriterOutputStream(Writer writer) {
        this.writer = writer;
    }

    @Override
    public void write(int b) throws IOException {
        writer.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        writer.write(new String(b));
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        writer.write(new String(b,off,len));
    }

    @Override
    public void flush() throws IOException {
        writer.flush();
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
