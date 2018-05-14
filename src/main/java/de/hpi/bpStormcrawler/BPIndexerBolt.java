package de.hpi.bpStormcrawler;



import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.util.Date;
import java.util.Map;

import com.digitalpebble.stormcrawler.elasticsearch.bolt.IndexerBolt;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
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

public class BPIndexerBolt extends IndexerBolt {
    private OutputCollector collector;
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());


    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        super.prepare(conf,context,collector);
        setCollector(collector);
    }

    @Override
    public void execute(Tuple tuple) {
        super.execute(tuple);


        // Distinguish the value used for indexing
        // from the one used for the status
        String normalisedUrl = valueForURL(tuple);
        Metadata metadata = (Metadata)tuple.getValueByField("metadata");


        long shopID = 0L;
        try{
            shopID = Long.parseLong(metadata.getFirstValue("shopId"));
        }
        catch (Exception e)
        {
            LOG.error("Could not get shopID", e);
        }


        //BP: added Content Field
        String content = new String(tuple.getBinaryByField("content"));


        //TODO extract the fetchedTime from metadata (the field name is date)

        this.collector.emit("storage", tuple, new Values(shopID,new Date(),normalisedUrl,content));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        declarer.declareStream("storage", new Fields("shopId", "fetchedDate", "url","content"));
    }


}