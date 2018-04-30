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
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryBuilders.*;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
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
        if (TupleUtils.isTick(tuple)) {
            for (String shopName : queryShopsAboveThreshold()) {
                collector.emit("finishedDomainNotification", tuple, new Values(shopName, new Date()));
            }
        }


        //TODO: Iteration 2: Query: Give me one tuple per hostname where there are no tuples with status DISCOVERED
    }

    private List<String> queryShopsAboveThreshold() {

        //Actual Query
        //More in Java: Give me the oldest tuple per hostname with the latest fetchDate

        //More on Elasticsearch:
        //Give me one tuple per hostname where the latest fetchDate is bigger than fetchDate + threshold


        SearchRequest request = new SearchRequest(getIndexName()).types(getDocType());
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        //REST QUERY
        //POST status/_search
        //{
        //  "aggs": {
        //      "hostName" :{
        //        "terms": {
        //          "field": "metadata.hostname"
        //        },
        //
        //        "aggs": {
        //          "numberDiscoveredLinks": {
        //            "filter": {
        //              "term": {
        //                "status": "DISCOVERED"
        //              }
        //            },
        //            "aggs": {
        //              "countOfDiscoveredLinks": {
        //                "value_count": {
        //                  "field": "status"
        //                }
        //
        //              }
        //            }
        //          }
        //        }
        //      }
        //    }
        //    ,"size": 0
        //}

        AggregationBuilder aggregation =
                AggregationBuilders
                        .terms("hostName")
                        .field("metadata.hostname")
                        .subAggregation(
                                AggregationBuilders
                                        .filter("discoveredLinks",QueryBuilders.termQuery("status","DISCOVERED"))
                                        .subAggregation(AggregationBuilders.cardinality("status"))
                        );
        sourceBuilder.aggregation(aggregation);
        sourceBuilder.size(0);
        sourceBuilder.explain(false);
        request.source(sourceBuilder);

        long start = System.currentTimeMillis();

        SearchResponse response;
        try{
            response = connection.getClient().search(request);
        } catch (IOException e){
            LOG.error("Exception caught when quering Elasticsearch in finished Domain Bolt ",e);
            collector.reportError(e);
            return null;
        }

        long end = System.currentTimeMillis();

        LOG.info("Query returned in {} msec", end - start);

        Terms domains = response.getAggregations().get("hostname");

        List<String> resultList = new ArrayList<>();

        for (Terms.Bucket domain : domains.getBuckets()) {
            Filter filter = domain.getAggregations().get("discoveredLinks");
            if(filter.getDocCount() == 0){
                 resultList.add(domain.getKeyAsString());
                 LOG.info("ADDED key [{}], doc_count [{}]", domain.getKeyAsString(), filter.getDocCount());
            }else{
                LOG.info("SKIPPED key [{}], doc_count [{}]", domain.getKeyAsString(), filter.getDocCount());
            }
        }
        return resultList;
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
        declarer.declareStream("finishedDomainNotification", new Fields("shopName", "finishedDate"));

    }
}
