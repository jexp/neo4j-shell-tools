package org.neo4j.shell.tools.imp.util;

import java.io.FilterWriter;
import java.io.IOException;
import java.io.Writer;

/**
 * @author mh
 * @since 17.01.14
 */
public class CountingWriter extends FilterWriter implements SizeCounter {
    private long count=0;
    private long newLines=0;
    protected CountingWriter(Writer out) {
        super(out);
    }

    @Override
    public void write(int c) throws IOException {
        super.write(c);
        count++;
        if (c == '\n') newLines++;
    }

    @Override
    public void write(char[] cbuf, int off, int len) throws IOException {
        super.write(cbuf, off, len);
        count+=len;
        for (int i=off;i<off+len;i++) {
            if (cbuf[i] == '\n') newLines++;
        }
    }

    public long getCount() {
        return count;
    }

    @Override
    public long getTotal() {
        return 0;
    }

    @Override
    public long getPercent() {
        return 0;
    }

    public long getNewLines() {
        return newLines;
    }
}
