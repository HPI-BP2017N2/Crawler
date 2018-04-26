package de.hpi.bpStormcrawler;

import com.digitalpebble.stormcrawler.elasticsearch.ElasticSearchConnection;
import com.digitalpebble.stormcrawler.elasticsearch.metrics.StatusMetricsBolt;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
@Getter(AccessLevel.PRIVATE)
@Setter(AccessLevel.PRIVATE)

public class BPFinishedDomainBolt extends BaseRichBolt{
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());
    private static final String ESBoltType = "status";
    private static final String ESStatusIndexNameParamName = "es.status.index.name";
    private static final String ESStatusDocTypeParamName = "es.status.doc.type";

    private String indexName;
    private String docType;

    private int updateInterval;
    private int waitingThresholdDomain;

    private OutputCollector collector;

    private ElasticSearchConnection connection;

    @Override
    public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {
        setCollector(outputCollector);

        setUpdateInterval(ConfUtils.getInt(stormConf,"finishedDomain.update.interval",60));
        setIndexName(ConfUtils.getString(stormConf, ESStatusIndexNameParamName,
                "status"));
        setDocType(ConfUtils.getString(stormConf, ESStatusDocTypeParamName,
                "doc"));
        setWaitingThresholdDomain(ConfUtils.getInt(stormConf,"finishedDomain.waitingThreshold",3));

        try {
            setConnection(ElasticSearchConnection.getConnection(stormConf, ESBoltType));
        }catch(Exception e){
            getLOG().error("Can't connect to Elasticsearch",e);
            collector.reportError(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        //Discard other incomming tuples but acknowledge them
        getCollector().ack(tuple);


        // this bolt can be connected to anything
        // we just want to trigger a new search when the input is a tick tuple
        if (!TupleUtils.isTick(tuple)) {
            return;
        }

        for (Long shopID : queryShopsAboveThreshold()) {
            collector.emit("finishedDomainNotification", tuple, new Values(1234L));
        }


        //TODO: Iteration 2: Query: Give me one tuple per hostname where there are no tuples with status DISCOVERED
    }

    private List<Long> queryShopsAboveThreshold() {
        /*SearchRequest request = new SearchRequest(getIndexName()).types(getDocType());
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        //Actual Query
        //More in Java: Give me the oldest tuple per hostname with the latest fetchDate


        //More on Elasticsearch:
        //Give me one tuple per hostname where the latest fetchDate is bigger than fetchDate + threshold


        //sourceBuilder.query(QueryBuilders.termQuery("status", s.name()));
        //TODO Implement query

        sourceBuilder.from(0);
        sourceBuilder.size(0);
        sourceBuilder.explain(false);
        request.source(sourceBuilder);

        //TODO Implement query submission

        //TODO Implement query analyze
        */

        List<Long> l = new ArrayList<>();
        l.add(1234L);
        l.add(4321L);
        return l;
    }

    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put("topology.tick.tuple.freq.secs", getUpdateInterval());
        return conf;
    }

    @Override
    public void cleanup() {
        getConnection().close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("finishedDomainNotification", new Fields("shopId"));

    }
}
