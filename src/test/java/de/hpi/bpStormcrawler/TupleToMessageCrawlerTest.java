package de.hpi.bpStormcrawler;

import net.bytebuddy.utility.RandomString;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

import static org.mockito.Mockito.*;

import static org.junit.Assert.*;

public class TupleToMessageCrawlerTest {

    private String randomString;

    @Before
    public void setup(){
         randomString = RandomStringUtils.random(8);
    }

    @Test
    public void extractBody() {
        //Test a strange tuple if it is converted in JSON
        //
        TupleToMessageCrawler scheme = spy(new TupleToMessageCrawler(randomString, randomString));

        Tuple testTuple = mock(Tuple.class);

        when(testTuple.getFields()).thenReturn(new Fields("test"));
        byte[] result = scheme.extractBody(testTuple);

        assertNotNull(result);

        //TODO Implement further Test Cases

    }



    @Test
    public void determineExchangeName() {
        String exchangeName= "abc";
        TupleToMessageCrawler scheme = new TupleToMessageCrawler(exchangeName, randomString);
        Tuple tuple = mock(Tuple.class);
        assertEquals(exchangeName, scheme.determineExchangeName(tuple));
    }

    @Test
    public void determineRoutingKey() {
        String routingName = "abc";
        TupleToMessageCrawler scheme = new TupleToMessageCrawler(randomString,routingName);
        Tuple tuple = mock(Tuple.class);
        assertEquals(routingName, scheme.determineRoutingKey(tuple));
    }
}