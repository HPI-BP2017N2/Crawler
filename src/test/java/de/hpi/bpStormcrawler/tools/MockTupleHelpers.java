package de.hpi.bpStormcrawler.tools;

import org.apache.storm.tuple.Tuple;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class MockTupleHelpers {

    private MockTupleHelpers() {
    }

    public static Tuple mockTickTuple() {
        return mockTuple("__system", "__tick");
    }

    public static Tuple mockTuple(String componentId, String streamId) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceComponent()).thenReturn(componentId);
        when(tuple.getSourceStreamId()).thenReturn(streamId);
        return tuple;
    }
}