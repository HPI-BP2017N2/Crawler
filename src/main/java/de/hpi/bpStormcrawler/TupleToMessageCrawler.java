package de.hpi.bpStormcrawler;

import io.latent.storm.rabbitmq.TupleToMessage;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;


/*
* A message Theme for the parsed Pages for the RabbitMQ
* @author jonaspohlmann
* */
public class TupleToMessageCrawler extends TupleToMessage {

    private String EXCHANGE_NAME;

    TupleToMessageCrawler (String exchangeName) {
        EXCHANGE_NAME = exchangeName;
    }

    @Override
    protected byte[] extractBody(Tuple tuple) {
        byte[] payload = null;
        try {
            //TODO Refactor to be more generic
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            builder.field("shopID",tuple.getValueByField("shopID"));
            builder.field("fetchedDate", tuple.getValueByField("fetchedDate"));
            builder.field("url",tuple.getValueByField("url"));
            builder.field("content", tuple.getValueByField("content"));
            builder.endObject();
            payload = builder.string().getBytes();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return payload;
    }

    @Override
    protected String determineExchangeName(Tuple tuple) {
        return EXCHANGE_NAME;
    }

    @Override
    protected String determineRoutingKey(Tuple input) {
        return "HtmlPagesToParse"; // rabbitmq java client library treats "" as no routing key
    }


}
