package com.guicedee.rabbit;

import com.guicedee.client.IGuiceContext;
import com.rabbitmq.client.AMQP;
import io.vertx.core.buffer.Buffer;
import io.vertx.rabbitmq.RabbitMQPublisher;
import jakarta.inject.Inject;

public class QueuePublisher
{
    @Inject
    private RabbitMQPublisher rabbitMQPublisher;

    private QueueDefinition queueDefinition;
    private String exchangeName;
    private String routingKey;

    public QueuePublisher(QueueDefinition queueDefinition, String exchangeName, String routingKey)
    {
        this.queueDefinition = queueDefinition;
        this.exchangeName = exchangeName;
        this.routingKey = routingKey;
    }

    public void publish(String body)
    {
        if(rabbitMQPublisher == null)
        {
            IGuiceContext.instance()
                         .inject()
                         .injectMembers(this);
        }
        AMQP.BasicProperties.Builder properties =  new AMQP.BasicProperties.Builder();
        if (queueDefinition.options()
                           .priority() != 0)
        {
            properties.priority(queueDefinition.options().priority());
        }
        rabbitMQPublisher.publish(exchangeName, routingKey, properties.build(), Buffer.buffer(body));
    }
}
