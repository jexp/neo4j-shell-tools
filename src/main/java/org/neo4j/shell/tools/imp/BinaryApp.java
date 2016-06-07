package org.neo4j.shell.tools.imp;

import org.neo4j.shell.impl.AbstractApp;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by efulton on 6/6/16.
 */
public abstract class BinaryApp extends AbstractApp {
  protected static final String KRYO = "kryo";
  protected static final String CBOR = "cbor";
  protected static final String PACKSTREAM = "packstream";
  protected static final Set<String> BINARY_FORMATS = new HashSet<>(Arrays.asList(KRYO, CBOR, PACKSTREAM));
  protected static final int MAX_FORMAT_STRING_SIZE = 10;
}
