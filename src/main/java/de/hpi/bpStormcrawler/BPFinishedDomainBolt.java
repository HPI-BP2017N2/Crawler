package de.hpi.bpStormcrawler;

import com.digitalpebble.stormcrawler.elasticsearch.ElasticSearchConnection;
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
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

@Getter(AccessLevel.PRIVATE)
@Setter(AccessLevel.PRIVATE)

public class BPFinishedDomainBolt extends BaseRichBolt{
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());
    private static final String ESBoltType = "status";
    private static final String ESStatusIndexNameParamName = "es.status.index.name";
    private static final String ESStatusDocTypeParamName = "es.status.doc.type";

    private Map<String, Boolean> finishedDomains;
    private String indexName;
    private String docType;

    private int updateInterval = 30;
    private int waitingThresholdDomain;

    private OutputCollector collector;

    private ElasticSearchConnection connection;

    @Override
    @SuppressWarnings("unchecked")
    public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {

        setCollector(outputCollector);

        //Is currently not used as the getComponentConfiguration is called before
        setUpdateInterval(ConfUtils.getInt(stormConf,"finishedDomain.update.interval",60));

        setIndexName(ConfUtils.getString(stormConf, ESStatusIndexNameParamName, "status"));
        setDocType(ConfUtils.getString(stormConf, ESStatusDocTypeParamName,"doc"));
        setWaitingThresholdDomain(ConfUtils.getInt(stormConf,"finishedDomain.waitingThreshold",100));

        setFinishedDomains(new Hashtable<>());

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
                Date currentTimestamp = new Date();
                collector.emit("finishedDomainNotification", tuple, new Values(shopName, currentTimestamp));
                LOG.info("Emmited Tuple with shopName {} and the timestamp {} ",shopName,currentTimestamp);
            }
        }
    }

    private List<String> queryShopsAboveThreshold() {
        SearchRequest request = buildAggregationQuery(queryDomainsWithUrlsDiscovered());
        SearchResponse response = queryElasticSearch(request);
        return extractNewFinishedDomains(response);
    }

    private List<String> extractNewFinishedDomains(SearchResponse response) {
        if (response != null) {
            Terms domains = response.getAggregations().get("hostName");
            List<String> resultList = new ArrayList<>();

            if (domains != null) {
                for (Terms.Bucket domain : domains.getBuckets()) {
                    if (isNewFinishedDomain(domain)) {
                        resultList.add(domain.getKeyAsString());
                    }
                }
            } else {
                LOG.warn("Couldn't find matching hostnames in SearchResponse");
            }
            return resultList;
        } else {
            return null;
        }
    }


    private Boolean isNewFinishedDomain(Terms.Bucket domain) {
        Filter filter = domain.getAggregations().get("discoveredLinks");
        String domainName = domain.getKeyAsString();
        if(filter.getDocCount() == 0){
            if(!getFinishedDomains().getOrDefault(domainName,false))
            {
                getFinishedDomains().put(domainName,true);
                LOG.info("ADDED key [{}], doc_count [{}]", domainName, filter.getDocCount());
                return true;
            } else{
                LOG.info("SKIPPED key [{}], doc_count [{}] because already emmited", domainName, filter.getDocCount());
            }
        }else{
            LOG.info("SKIPPED key [{}], doc_count [{}] because criteria not yet met", domainName, filter.getDocCount());
        }
        return false;
    }

    private SearchResponse queryElasticSearch (SearchRequest request){
        SearchResponse response;
        long start = System.currentTimeMillis();

        try{
            response = connection.getClient().search(request);
        } catch (IOException e){
            LOG.error("Exception caught when quering Elasticsearch in finished Domain Bolt ",e);
            collector.reportError(e);
            return null;
        }
        long end = System.currentTimeMillis();

        LOG.info("Query returned in {} msec", end - start);
        return response;
    }

    private SearchRequest buildAggregationQuery(AggregationBuilder query) {
        SearchRequest request = new SearchRequest(getIndexName()).types(getDocType());
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(query);
        sourceBuilder.size(0);
        sourceBuilder.explain(false);
        request.source(sourceBuilder);
        return request;
    }

    private AggregationBuilder queryDomainsWithUrlsDiscovered() {
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
        //            }
        //          }
        //        }
        //      }
        //    }
        //    ,"size": 0
        //}
        return AggregationBuilders
                .terms("hostName")
                .field("metadata.hostname")
                .subAggregation(
                        AggregationBuilders
                                .filter("discoveredLinks", QueryBuilders.termQuery("status","DISCOVERED"))
                );
    }

    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put("topology.tick.tuple.freq.secs", getUpdateInterval());
        LOG.info("Set update Interval of Tick Tuples to {}",getUpdateInterval());
        //TODO Implement that it gets the frequency from the config file
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
