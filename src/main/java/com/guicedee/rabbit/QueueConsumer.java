package com.guicedee.rabbit;

import io.vertx.rabbitmq.RabbitMQMessage;

public interface QueueConsumer
{
    /**
     * Handles a message
     *
     * @throws Exception The message to handler
     */
    void consume(RabbitMQMessage message);
}
