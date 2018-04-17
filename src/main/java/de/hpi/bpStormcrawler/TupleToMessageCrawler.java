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
    private String ROUTING_NAME;

    TupleToMessageCrawler (String exchangeName, String routingName) {
        EXCHANGE_NAME = exchangeName;
        ROUTING_NAME = routingName;
    }

    @Override
    protected byte[] extractBody(Tuple tuple) {
        byte[] payload = null;
        try {
            //TODO Refactor to be more generic
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            for (String element : tuple.getFields()){
                builder.field(element,tuple.getValueByField(element));
            }

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
        return ROUTING_NAME; // rabbitmq java client library treats "" as no routing key
    }


}
