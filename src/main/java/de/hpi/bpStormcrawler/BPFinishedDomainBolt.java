package de.hpi.bpStormcrawler;

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
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Getter(AccessLevel.PRIVATE)
@Setter(AccessLevel.PRIVATE)

public class BPFinishedDomainBolt extends BaseRichBolt{
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());


    private Map<String, Boolean> finishedDomains;


    private BPStatusElasticSearch elasticSearch;

    private int updateInterval = 30;
    private int waitingThresholdDomain;

    private OutputCollector collector;



    @Override
    @SuppressWarnings("unchecked")
    public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {

        setCollector(outputCollector);

        setElasticSearch(new BPStatusElasticSearch(stormConf));

        setFinishedDomains(new Hashtable<>());

    }



    @Override
    public void execute(Tuple tuple) {
        //Discard other incomming tuples but acknowledge them
        getCollector().ack(tuple);

        // this bolt can be connected to anything
        // we just want to trigger a new search when the input is a tick tuple
        if (TupleUtils.isTick(tuple)) {
            try {
                for (String shopName : getRecentlyFinishedDomains()) {
                    Date currentTimestamp = new Date();
                    collector.emit("finishedDomainNotification", tuple, new Values(shopName, currentTimestamp));
                    LOG.info("Emmited Tuple with shopName {} and the timestamp {} ",shopName,currentTimestamp);
                }
            } catch (IOException e) {
                getLOG().error("Exception caught when quering Elasticsearch in finished Domain Bolt ",e);
                getCollector().reportError(e);
            }
        }
    }

    private List<String> getRecentlyFinishedDomains() throws IOException {
        List<String> finishedDomains = getElasticSearch().getFinishedDomains();

        List<String> recentlyFinishedDomains = finishedDomains.stream()
                .filter(domain -> !getFinishedDomains().getOrDefault(domain, false))
                .collect(Collectors.toList());
        recentlyFinishedDomains.forEach(domain-> {
            getFinishedDomains().put(domain,true);
            LOG.info("ADDED key [{}]", domain);
        });
        return recentlyFinishedDomains;
    }

    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put("topology.tick.tuple.freq.secs", getUpdateInterval());
        LOG.info("Set update Interval of Tick Tuples to {}",getUpdateInterval());
        return conf;
    }

    @Override
    public void cleanup() {
        getElasticSearch().close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("finishedDomainNotification", new Fields("shopName", "finishedDate"));
    }
}
