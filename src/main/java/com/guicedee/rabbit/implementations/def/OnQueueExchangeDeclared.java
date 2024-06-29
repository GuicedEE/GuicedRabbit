package com.guicedee.rabbit.implementations.def;

import io.vertx.rabbitmq.RabbitMQClient;

public interface OnQueueExchangeDeclared
{
    void perform(RabbitMQClient client, String exchangeName);
}
