package de.hpi.bpStormcrawler;

import de.hpi.bpStormcrawler.tools.MockTupleHelpers;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.verification.VerificationMode;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.*;

public class BPFinishedDomainBoltTest {
    private BPFinishedDomainBolt bolt;
    private OutputCollector collector;
    private BPStatusElasticSearch elasticSearch;

    @Before
    public void setup(){
        bolt = new BPFinishedDomainBolt();
        collector = mock(OutputCollector.class);
        elasticSearch = mock(BPStatusElasticSearch.class);
        bolt.setCollector(collector);
        bolt.setElasticSearch(elasticSearch);
        bolt.setFinishedDomains(new Hashtable<>());
    }


    @Test
    public void ignoreNonTickTuples(){
        Tuple nonTickTuple = MockTupleHelpers.mockTuple("a","b");
        bolt.execute(nonTickTuple);
        verify(collector,times(0)).emit(anyString(), any(Tuple.class), any());
    }

    @Test
    public void emitTupleWhenDomainFinished() throws IOException {
        Tuple tickTuple = MockTupleHelpers.mockTickTuple();

        List<String> finishedDomains = new ArrayList<>();
        finishedDomains.add("google.com");
        doReturn(finishedDomains).when(elasticSearch).getFinishedDomains();
        bolt.execute(tickTuple);
        verify(collector,times(1)).emit(anyString(),any(Tuple.class),any());
    }

    @Test
    public void emitNoTupleWhenTupleOfFinishedDomainWasAlreadyEmitted() throws IOException {
        Tuple tickTuple = MockTupleHelpers.mockTickTuple();
        List<String> finishedDomains = new ArrayList<>();
        finishedDomains.add("google.com");
        finishedDomains.add("alternate.com");
        doReturn(finishedDomains).when(elasticSearch).getFinishedDomains();
        bolt.execute(tickTuple);

        finishedDomains.add("idealo.com");
        doReturn(finishedDomains).when(elasticSearch).getFinishedDomains();
        bolt.execute(tickTuple);
        verify(collector,times(3)).emit(anyString(),any(Tuple.class),any());

    }

    @Test
    public void notEmmitTupleWhenNoShopisFinished() throws IOException {
        Tuple tickTuple = MockTupleHelpers.mockTickTuple();
        List<String> finishedDomains = new ArrayList<>();
        doReturn(finishedDomains).when(elasticSearch).getFinishedDomains();
        bolt.execute(tickTuple);
        verify(collector, never()).emit(anyString(),any(Tuple.class),any());

    }


}