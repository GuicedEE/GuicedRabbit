package com.guicedee.rabbit;

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.vertx.core.Vertx;
import io.vertx.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQPublisher;
import io.vertx.rabbitmq.RabbitMQPublisherOptions;
import lombok.extern.java.Log;

@Log
public class RabbitMQPublisherProvider implements Provider<RabbitMQPublisher> {
    private RabbitMQClient client;

    @Inject
    private Vertx vertx;

    private RabbitMQPublisher publisher = null;

    public void setClient(RabbitMQClient client) {
        this.client = client;
        client.addConnectionEstablishedCallback((est) -> {
            publisher = RabbitMQPublisher.create(vertx, client, new RabbitMQPublisherOptions());
        });
    }

    @Override
    public RabbitMQPublisher get() {
        return publisher;
    }

}
