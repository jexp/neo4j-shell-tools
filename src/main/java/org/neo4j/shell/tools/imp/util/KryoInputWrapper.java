package org.neo4j.shell.tools.imp.util;

import com.esotericsoftware.kryo.io.Input;

/**
 * Created by efulton on 5/9/16.
 */
public class KryoInputWrapper implements SizeCounter {

    private final Input input;
    private final long size;

    public KryoInputWrapper(Input input, long size) {
        this.input = input;
        this.size = size;
    }

    @Override
    public long getNewLines() {
        return 0l;
    }

    @Override
    public long getCount() {
        return input.total();
    }

    @Override
    public long getTotal() {
        return size;
    }

    @Override
    public long getPercent() {
        if (size <= 0) {
            return 0;
        } else {
            return getCount() * 100 / size;
        }
    }
}