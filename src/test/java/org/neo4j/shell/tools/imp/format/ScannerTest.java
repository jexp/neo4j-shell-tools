package org.neo4j.shell.tools.imp.format;

import org.junit.Test;

import java.util.Scanner;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 18.01.14
 */
public class ScannerTest {

    @Test
    public void testReadCypherFile() throws Exception {
        Scanner scanner = new Scanner("begin\nstart;\ncommit\nbegin\nstart;\ncommit").useDelimiter(Pattern.compile("(commit|begin|;)(\r?\n|$)", Pattern.CASE_INSENSITIVE));
        System.out.println("scanner.delimiter() = " + scanner.delimiter());
        assertEquals("start",scanner.next());
        assertEquals("",scanner.next());
        assertEquals("",scanner.next());
        assertEquals("start",scanner.next());
        assertEquals("",scanner.next());
        assertEquals(false,scanner.hasNext());
    }
}
