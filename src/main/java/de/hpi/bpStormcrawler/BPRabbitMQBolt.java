package de.hpi.bpStormcrawler;

import io.latent.storm.rabbitmq.Declarator;
import io.latent.storm.rabbitmq.RabbitMQBolt;
import io.latent.storm.rabbitmq.TupleToMessage;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class BPRabbitMQBolt extends RabbitMQBolt {

    /** This constructor is used to configure the exchange name and the routing name of RabbitMQ in the constructor
     * The parameters are given through flux and therefore easy changable
     *
     * Another idea would be to set these parameters in the storm configuration. The only problem is then to distingish
     * between multiple instances of this component which is for what.
     *
     * @param exchangeName The RabbitMQ exchange name which can be configured in RabbitMQ UI under Exchanges
     * @param routingName The RabbitMQ routing name which is used to determine in a Exchange where the message should be
     *                    routed to
     */
    public BPRabbitMQBolt(String exchangeName, String routingName) {
        super(new TupleToMessageCrawler(exchangeName, routingName));
    }

    /**This method is implemented to keep the functionality of the super class
     * @param scheme The scheme on how tuples which arrive should be handled and translated to a message
     * @param declarator Used in the super method
     */
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
