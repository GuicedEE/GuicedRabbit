package com.guicedee.rabbit.implementations;

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.vertx.core.Vertx;
import io.vertx.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQPublisher;
import io.vertx.rabbitmq.RabbitMQPublisherOptions;
import jakarta.inject.Singleton;
import lombok.extern.java.Log;

import java.util.concurrent.TimeUnit;

import static com.guicedee.rabbit.QueuePublisher.done;


@Log
@Singleton
public class RabbitMQPublisherProvider implements Provider<RabbitMQPublisher>
{
    @Inject
    private RabbitMQClient client;
    @Inject
    private Vertx vertx;

    private RabbitMQPublisher publisher = null;

    @Override
    public RabbitMQPublisher get()
    {
        if (publisher == null)
        {
            if (done.isDone())
            {
                publisher = RabbitMQPublisher.create(vertx, client, new RabbitMQPublisherOptions());
            }
            else
            {
                done.thenRun(() -> {
                    if (!client.isConnected())
                    {
                        client.addConnectionEstablishedCallback((est) -> {
                            publisher = RabbitMQPublisher.create(vertx, client, new RabbitMQPublisherOptions());
                        });
                    }
                    else
                    {
                        publisher = RabbitMQPublisher.create(vertx, client, new RabbitMQPublisherOptions());

                    }
                });
            }
        }
        while (publisher == null)
        {
            try
            {
                TimeUnit.MILLISECONDS.sleep(100);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }

        }
        return publisher;
    }

}
