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

    /** This method is called once and prepares the component to be used. It configures the component in setting
     * the Elasticsearch component and the InMemory store for the finished shops
     *
     * @param stormConf The configuration we get from the .yaml files as a Map
     * @param topologyContext not used in the method but required from the Interface
     * @param outputCollector The collector which is used to emmit tuples into the topology
     */
    @Override
    @SuppressWarnings("unchecked")
    public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {
        setCollector(outputCollector);
        setElasticSearch(new BPStatusElasticSearch(stormConf));
        setFinishedShops(new HashSet<>());
    }


    /** This message is executed every time a tuple arrives.
     * A tick tuple triggers that we get a list of recently finished shops and emits a tuple
     * with the shopId and the currentTimestamp to the next component for every shop which was recently finished crawling.
     * Recently finished means here that we get from Elasticsearch all the shops which are finished and substract them
     * from the shops (shopIds) we already emmited. Every time we store when we emmit a shop that it is not emmitted again
     *
     * Limitations:
     * - When restarting the component we loose information which shopIds we already emmited as
     * this is stored in memory within the component
     *
     * - With the decision to continously crawl a shop it would most likely never recognize when a shop is crawled
     *
     *
     * @param tuple The tuple that arrives from another component or storm itself
     */
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
     * Set Storm configuration for this component. We set the configuration that this component gets a tick tuple
     * in the updateInterval.
     *
     * NOTE: You cannot get interval from the Storm configuration as this method gets called before the prepare method
     *
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {

        Config config = new Config();

        config.put(UPDATE_INTERVAL_IN_SECONDS_KEY, getUpdateInterval());
        log.info("Set update interval of tick tuples to {}", getUpdateInterval());
        return config;
    }

    /**
     * This is the cleanup method which gets calles before the component is terminated
     * It closes the established Elasticsearch connection
     */
    @Override
    public void cleanup() {
        getElasticSearch().close();
    }



    /** We declare how a tuple this component emits looks like. We already also specify the streamId in which we emmit
     * this tuple into
     *
     * @param declarer is the declarer which is used to configure which tuples the component emmits
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(FINISHED_SHOPS_NOTIFICATION, new Fields(SHOP_NAME, FINISHED_DATE));
    }
}
