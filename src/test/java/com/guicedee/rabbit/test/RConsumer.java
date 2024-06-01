package com.guicedee.rabbit.test;

import com.guicedee.rabbit.QueueConsumer;
import com.guicedee.rabbit.QueueDefinition;
import com.guicedee.rabbit.RabbitConnectionOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.rabbitmq.RabbitMQMessage;
import jakarta.inject.Singleton;

@RabbitConnectionOptions(value = "connection-1",
                         password = "guest",
                         automaticRecoveryEnabled = false,
                         reconnectAttempts = 2,
                         reconnectInterval = 500)
@QueueDefinition(value = "test-queue-consumer")
@Singleton
public class RConsumer implements QueueConsumer
{

    @Override
    public void consume(RabbitMQMessage message)
    {
        Buffer body = message.body();
        System.out.println("Consumed - " + body.toString());
    }
}
