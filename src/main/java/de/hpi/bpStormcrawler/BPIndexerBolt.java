package de.hpi.bpStormcrawler;



import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.util.Date;
import java.util.Map;

import com.digitalpebble.stormcrawler.elasticsearch.bolt.IndexerBolt;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.digitalpebble.stormcrawler.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends documents to ElasticSearch. Indexes all the fields from the tuples or a
 * Map &lt;String,Object&gt; from a named field.
 */
@SuppressWarnings("serial")
@Getter (AccessLevel.PRIVATE)
@Setter (AccessLevel.PRIVATE)
@Slf4j
public class BPIndexerBolt extends IndexerBolt {
    private OutputCollector collector;


    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        super.prepare(conf, context, collector);
        setCollector(collector);
    }

    @Override
    public void execute(Tuple tuple) {
        super.execute(tuple);

        String uniformedUrl = getUniformedUrl(tuple);
        Metadata metadata = (Metadata) tuple.getValueByField("metadata");
        long shopID = Long.parseLong(metadata.getFirstValue("shopId"));
        String content = new String(tuple.getBinaryByField("content"));

        //TODO extract the fetchedTime from metadata (the field name is date)

        getCollector().emit("storage", tuple, new Values(shopID, new Date(), uniformedUrl, content));
    }

    private String getUniformedUrl(Tuple tuple) {
        return valueForURL(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        declarer.declareStream("storage", new Fields("shopId", "fetchedDate", "url","content"));
    }


}