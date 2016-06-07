package org.neo4j.shell.tools.imp.util;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * Created by efulton on 5/9/16.
 */
public class FileInputWrapper implements SizeCounter {

    private final FileInputStream fileInputStream;

    public FileInputWrapper(FileInputStream fileInputStream) {
        this.fileInputStream = fileInputStream;
    }

    @Override
    public long getNewLines() {
        return 0l;
    }

    @Override
    public long getCount() {
        long position = 0l;
        try {
            position = fileInputStream.getChannel().position();
        } catch (IOException e) {
        }
        return position;
    }

    @Override
    public long getTotal() {
        long size = 0l;
        try {
            size = fileInputStream.getChannel().size();
        } catch (IOException e) {
        }
        return size;
    }

    @Override
    public long getPercent() {
        if (getTotal() <= 0) {
            return 0;
        } else {
            return getCount() * 100 / getTotal();
        }
    }
}