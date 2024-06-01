package com.guicedee.rabbit.test;

import com.guicedee.rabbit.QueueConsumer;
import com.guicedee.rabbit.QueueDefinition;
import com.guicedee.rabbit.QueueOptions;
import io.vertx.rabbitmq.RabbitMQMessage;

@QueueDefinition(value = "single-consumer-test",options = @QueueOptions(singleConsumer = true))
public class SingleConsumer implements QueueConsumer
{
    @Override
    public void consume(RabbitMQMessage message)
    {
        System.out.println("Single consumer got - " + message.body());
        System.exit(0);
    }
}
