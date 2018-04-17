package de.hpi.bpStormcrawler;



import static com.digitalpebble.stormcrawler.Constants.StatusStreamName;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.*;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.digitalpebble.stormcrawler.elasticsearch.bolt.IndexerBolt;
import com.google.common.annotations.VisibleForTesting;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.ConfigUtils;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.elasticsearch.ElasticSearchConnection;
import com.digitalpebble.stormcrawler.indexing.AbstractIndexerBolt;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.util.ConfUtils;

/**
 * Sends documents to ElasticSearch. Indexes all the fields from the tuples or a
 * Map &lt;String,Object&gt; from a named field.
 */
@SuppressWarnings("serial")
public class BPIndexerBolt extends IndexerBolt {
    private static final Logger LOG = LoggerFactory.getLogger(BPIndexerBolt.class);
    private OutputCollector _collector;


    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        super.prepare(conf,context,collector);
        this._collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        super.execute(tuple);

        String url = tuple.getStringByField("url");

        // Distinguish the value used for indexing
        // from the one used for the status
        String normalisedurl = valueForURL(tuple);

        Metadata metadata = (Metadata) tuple.getValueByField("metadata");
        String text = tuple.getStringByField("text");

        long shopID = 1234;

        //TODO: Implement shopID
        /*try{
            shopID = tuple.getLongByField("shopID");
        }
        catch (Exception e)
        {
            LOG.error("Could not get shopID", e);
        }*/


        //BP: added Content Field
        String content = new String(tuple.getBinaryByField("content"));



        //TODO Think about ack from the IndexerBolt
        //TODO extract the fetchedTime from metadata

        this._collector.emit("storage", tuple, new Values(shopID,System.currentTimeMillis(),normalisedurl,content));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        declarer.declareStream("storage", new Fields("shopID", "fetchedDate", "url","content"));
    }


}