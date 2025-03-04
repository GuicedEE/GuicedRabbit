package com.guicedee.rabbit;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.rabbitmq.client.AMQP;
import io.vertx.core.buffer.Buffer;
import io.vertx.rabbitmq.RabbitMQClient;
import lombok.EqualsAndHashCode;
import lombok.extern.log4j.Log4j2;

@JsonSerialize(as = Void.class)
@EqualsAndHashCode(of = {"routingKey"})
@Log4j2
public class QueuePublisher {

    private final RabbitMQClient client;
    private final boolean confirmPublish;
    private final QueueDefinition queueDefinition;

    private final String exchangeName;
    private final String routingKey;

    public QueuePublisher(RabbitMQClient client, boolean confirmPublish, QueueDefinition queueDefinition, String exchangeName, String routingKey) {
        this.client = client;
        this.confirmPublish = confirmPublish;
        this.queueDefinition = queueDefinition;
        this.exchangeName = exchangeName;
        this.routingKey = routingKey;
    }


    public void publish(String body) {
        sendMessage(client,confirmPublish,queueDefinition,exchangeName,routingKey,body);
    }

    private void sendMessage(RabbitMQClient client, boolean confirmPublish, QueueDefinition queueDefinition, String exchangeName, String routingKey, String body)
    {
        AMQP.BasicProperties.Builder properties = new AMQP.BasicProperties.Builder();
        if (queueDefinition.options()
                .priority() != 0)
        {
            properties.priority(queueDefinition.options()
                    .priority());
        }
        Buffer message = Buffer.buffer(body);

        if (confirmPublish)
        {
            log.trace("Message publishing to queue {} - {} / {}", routingKey, exchangeName, body);
            client.basicPublish(exchangeName, routingKey, message).onComplete(pubResult -> {
                if (pubResult.succeeded())
                {
                    // Check the message got confirmed by the broker.
                    client.waitForConfirms().onComplete(waitResult -> {
                        if (waitResult.succeeded())
                            log.trace("Message published to queue {} - {}", routingKey, exchangeName);
                        else
                            waitResult.cause().printStackTrace();
                    });
                } else
                {
                    pubResult.cause().printStackTrace();
                }
            });
        } else
            client.basicPublish(exchangeName, routingKey, message)
                    .onSuccess(v -> log.trace("Message published to queue {} - {}", routingKey, exchangeName))
                    .onFailure(t-> log.error("Failed to publish message to queue {} - {}", routingKey, exchangeName, t));
    }

}
