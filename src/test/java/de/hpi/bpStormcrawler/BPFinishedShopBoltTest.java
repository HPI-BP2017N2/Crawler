package de.hpi.bpStormcrawler;

import de.hpi.bpStormcrawler.tools.MockTupleHelpers;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.junit.Before;
import org.junit.Test;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;

import static org.mockito.Mockito.*;

public class BPFinishedShopBoltTest {
    private BPFinishedShopBolt bolt;
    private OutputCollector collector;
    private BPStatusElasticSearch elasticSearch;

    @Before
    public void setup(){
        bolt = new BPFinishedShopBolt();
        collector = mock(OutputCollector.class);
        elasticSearch = mock(BPStatusElasticSearch.class);
        bolt.setCollector(collector);
        bolt.setElasticSearch(elasticSearch);
        bolt.setFinishedShops(new HashSet<>());
    }


    @Test
    public void ignoreNonTickTuples(){
        Tuple nonTickTuple = MockTupleHelpers.mockTuple("a","b");
        bolt.execute(nonTickTuple);
        verify(collector,never()).emit(anyString(), any(Tuple.class), any());
    }

    @Test
    public void emitTupleWhenShopFinished() throws IOException {
        Tuple tickTuple = MockTupleHelpers.mockTickTuple();

        List<Long> finishedShops = new ArrayList<>();
        finishedShops.add(1234L);
        doReturn(finishedShops).when(elasticSearch).getFinishedShops();
        bolt.execute(tickTuple);
        verify(collector).emit(anyString(),any(Tuple.class),any());
    }

    @Test
    public void emitNoTupleWhenTupleOfFinishedShopWasAlreadyEmitted() throws IOException {
        Tuple tickTuple = MockTupleHelpers.mockTickTuple();
        List<Long> finishedShops = new ArrayList<>();
        finishedShops.add(1234L);
        finishedShops.add(5678L);
        doReturn(finishedShops).when(elasticSearch).getFinishedShops();
        bolt.execute(tickTuple);

        finishedShops.add(5432L);
        doReturn(finishedShops).when(elasticSearch).getFinishedShops();
        bolt.execute(tickTuple);
        verify(collector,times(3)).emit(anyString(),any(Tuple.class),any());

    }

    @Test
    public void notEmmitTupleWhenNoShopisFinished() throws IOException {
        Tuple tickTuple = MockTupleHelpers.mockTickTuple();
        List<Long> finishedShops = new ArrayList<>();
        doReturn(finishedShops).when(elasticSearch).getFinishedShops();
        bolt.execute(tickTuple);
        verify(collector, never()).emit(anyString(),any(Tuple.class),any());

    }


}