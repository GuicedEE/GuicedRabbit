package com.guicedee.rabbit;

import io.vertx.rabbitmq.RabbitMQMessage;

/**
 * Contract for consuming messages from a declared queue.
 */
public interface QueueConsumer
{
    /**
     * Handles a single RabbitMQ message.
     *
     * @param message The received message payload and metadata.
     */
    void consume(RabbitMQMessage message);
}
