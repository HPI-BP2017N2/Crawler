package de.hpi.bpStormcrawler;

import io.latent.storm.rabbitmq.Declarator;
import io.latent.storm.rabbitmq.RabbitMQBolt;
import io.latent.storm.rabbitmq.TupleToMessage;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class BPRabbitMQBolt extends RabbitMQBolt {

    public BPRabbitMQBolt(String exchangeName) {
        super(new TupleToMessageCrawler(exchangeName));
    }

    public BPRabbitMQBolt(TupleToMessage scheme, Declarator declarator) {
        super(scheme, declarator);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf,context,collector);
    }
    @Override
    public void execute(final Tuple tuple) {
        publish(tuple);
        acknowledge(tuple);
    }


}
