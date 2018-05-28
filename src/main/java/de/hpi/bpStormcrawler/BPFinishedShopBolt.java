package de.hpi.bpStormcrawler;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;

import java.io.IOException;
import java.util.*;

@Getter(AccessLevel.PRIVATE)
@Setter
@Slf4j
public class BPFinishedShopBolt extends BaseRichBolt {
    private static final String FINISHED_SHOPS_NOTIFICATION = "finishedShopsNotification";
    private static final String UPDATE_INTERVAL_IN_SECONDS_KEY = "topology.tick.tuple.freq.secs";
    private static final String FINISHED_DATE = "finishedDate";
    private static final String SHOP_NAME = "shopName";
    private Set<Long> finishedShops;
    private BPStatusElasticSearch elasticSearch;
    private int updateInterval = 30;
    private OutputCollector collector;

    @Override
    @SuppressWarnings("unchecked")
    public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {
        setCollector(outputCollector);
        setElasticSearch(new BPStatusElasticSearch(stormConf));
        setFinishedShops(new HashSet<>());
    }


    @Override
    public void execute(Tuple tuple) {
        getCollector().ack(tuple);

        if (TupleUtils.isTick(tuple)) {
            try {
                for (Long shopId : getRecentlyFinishedShops()) {
                    Date currentTimestamp = new Date();
                    getCollector().emit(FINISHED_SHOPS_NOTIFICATION, tuple, new Values(shopId, currentTimestamp));
                    log.info("Finished shop {} at {} ", shopId, currentTimestamp);
                }
            } catch (IOException e) {
                log.error("Exception caught when querying Elasticsearch", e);
                getCollector().reportError(e);
            }
        }
    }

    private List<Long> getRecentlyFinishedShops() throws IOException {
        List<Long> finishedShops = getElasticSearch().getFinishedShops();
        finishedShops.removeAll(getFinishedShops());
        getFinishedShops().addAll(finishedShops);
        return finishedShops;
    }

    /**
     * Set Storm configuration for this component
     * Cannot get interval from Storm configuration as this method gets called before the prepare method
     *
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {

        Config config = new Config();

        config.put(UPDATE_INTERVAL_IN_SECONDS_KEY, getUpdateInterval());
        log.info("Set update interval of tick tuples to {}", getUpdateInterval());
        return config;
    }

    @Override
    public void cleanup() {
        getElasticSearch().close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(FINISHED_SHOPS_NOTIFICATION, new Fields(SHOP_NAME, FINISHED_DATE));
    }
}
