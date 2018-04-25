package de.hpi.bpStormcrawler;

import net.bytebuddy.utility.RandomString;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;
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
        //Test conversion of regular Tuple to JSON

        TupleToMessageCrawler scheme = new TupleToMessageCrawler(randomString, randomString);

        Tuple testTuple = mock(Tuple.class);

        Fields fields = new Fields("shopID","fetchedDate","url","content");

        doReturn(fields).when(testTuple).getFields();
        doReturn(1234L).when(testTuple).getValueByField("shopID");
        doReturn(new Date(0L)).when(testTuple).getValueByField("fetchedDate");
        doReturn("http://www.google.de/").when(testTuple).getValueByField("url");
        doReturn("<html></html>").when(testTuple).getValueByField("content");

        byte[] result = scheme.extractBody(testTuple);

        assertEquals("{\"shopID\":1234,\"fetchedDate\":\"1970-01-01T00:00:00.000Z\",\"url\":\"http://www.google.de/\",\"content\":\"<html></html>\"}",new String(result));


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