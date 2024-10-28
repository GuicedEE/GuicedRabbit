package com.guicedee.rabbit;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.guicedee.rabbit.implementations.def.RabbitMQClientProvider;
import com.rabbitmq.client.AMQP;
import io.vertx.core.buffer.Buffer;
import lombok.EqualsAndHashCode;
import lombok.extern.java.Log;

@JsonSerialize(as = Void.class)
@EqualsAndHashCode(of = {"routingKey"})
@Log
public class QueuePublisher {
/*
    @Inject
    private RabbitMQPublisher rabbitMQPublisher;
*/

    RabbitMQClientProvider client;
    private final boolean confirmPublish;
    private QueueDefinition queueDefinition;

    private String exchangeName;
    private String routingKey;

    public QueuePublisher(RabbitMQClientProvider client, boolean confirmPublish, QueueDefinition queueDefinition, String exchangeName, String routingKey) {
        this.client = client;
        this.confirmPublish = confirmPublish;
        this.queueDefinition = queueDefinition;
        this.exchangeName = exchangeName;
        this.routingKey = routingKey;
    }


    public void publish(String body) {
        sendMessage(body);
    }

    private void sendMessage(String body) {
      /*  if (rabbitMQPublisher == null) {
            IGuiceContext.instance()
                    .inject()
                    .injectMembers(this);
        }*/
        AMQP.BasicProperties.Builder properties = new AMQP.BasicProperties.Builder();
        if (queueDefinition.options()
                .priority() != 0) {
            properties.priority(queueDefinition.options()
                    .priority());
        }
        Buffer message = Buffer.buffer(body);

        if (confirmPublish) {
            log.config("Message publishing to queue " + routingKey + " - " + exchangeName + " / " + body);
            client.get().basicPublish(exchangeName, routingKey, message, pubResult -> {
                if (pubResult.succeeded()) {
                    // Check the message got confirmed by the broker.
                    client.get().waitForConfirms(waitResult -> {
                        if (waitResult.succeeded())
                            log.config("Message published to queue " + routingKey + " - " + exchangeName);
                        else
                            waitResult.cause().printStackTrace();
                    });
                } else {
                    pubResult.cause().printStackTrace();
                }
            });
        } else
            client.get().basicPublish(exchangeName, routingKey, message);
    }

}
