package org.neo4j.shell.tools.imp.util;

import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 17.01.14
 */
public class QueryParameterExtractorTest {

    QueryParameterExtractor extractor = new QueryParameterExtractor();
    private HashMap<String,Object> params = new HashMap<>();

    @Test
    public void testExtractByte() throws Exception {
        assertEquals("MATCH (n {key:{` p 0`}}) return n", extractor.extract("MATCH (n {key:12}) return n", params));
        assertEquals((byte)12,params.get(" p 0"));
    }
    @Test
    public void testExtractMatchAndSet() throws Exception {
        assertEquals("MATCH (n {key:{` p 0`}}) SET n.key = {` p 1`}, n.key2 = {` p 2`} return n", extractor.extract("MATCH (n {key:12}) SET n.key = 43445545, n.key2 = 'abc' return n", params));
        assertEquals((byte)12,params.get(" p 0"));
        assertEquals(43445545,params.get(" p 1"));
        assertEquals("abc",   params.get(" p 2"));
    }
    @Test
    public void testExtractTwoParameters() throws Exception {
        assertEquals("MATCH (n {key:{` p 0`}, key2:{` p 1`}}) return n", extractor.extract("MATCH (n {key:12, key2:'abc'}) return n", params));
        assertEquals((byte)12,params.get(" p 0"));
        assertEquals("abc",params.get(" p 1"));
    }
    @Test
    public void testExtractLong() throws Exception {
        assertEquals("MATCH (n {key:{` p 0`}}) return n", extractor.extract("MATCH (n {key:12343434344}) return n", params));
        assertEquals(12343434344L,params.get(" p 0"));
    }

    @Test
    public void testExtractDoubleQuoteString() throws Exception {
        assertEquals("MATCH (n {key:{` p 0`}}) return n", extractor.extract("MATCH (n {key:\"abc\"}) return n", params));
        assertEquals("abc",params.get(" p 0"));
    }
    @Test
    public void testExtractDoubleQuoteStringWithEscape() throws Exception {
        assertEquals("MATCH (n {key:\"ab\\\"c\"}) return n", extractor.extract("MATCH (n {key:\"ab\\\"c\"}) return n", params));
        assertEquals(null,params.get(" p 0"));
    }
    @Test
    public void testExtractDoubleSingleString() throws Exception {
        assertEquals("MATCH (n {key:{` p 0`}}) return n", extractor.extract("MATCH (n {key:'abc'}) return n", params));
        assertEquals("abc",params.get(" p 0"));
    }
    @Test
    public void testExtractNull() throws Exception {
        assertEquals("MATCH (n {key:{` p 0`}}) return n", extractor.extract("MATCH (n {key:null}) return n", params));
        assertEquals(null,params.get(" p 0"));
    }

    @Test
    public void testPerformance() throws Exception {
        long time = System.currentTimeMillis();
        int counter = 100000;
        for (int i=0;i< counter;i++) {
            extractor.extract("MATCH (n {key:12}) SET n.key = 43445545, n.key2 = 'abc' return n", params);
        }
        time = System.currentTimeMillis() - time;
        System.out.println("time = " + time+" ms for "+counter);
    }
}
