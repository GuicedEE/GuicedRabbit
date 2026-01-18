package com.guicedee.rabbit.implementations.def;

import io.vertx.rabbitmq.RabbitMQClient;

/**
 * Callback invoked once an exchange has been declared.
 */
public interface OnQueueExchangeDeclared
{
    /**
     * Performs additional work after exchange declaration.
     *
     * @param client       The client used to declare the exchange.
     * @param exchangeName The declared exchange name.
     */
    void perform(RabbitMQClient client, String exchangeName);
}
