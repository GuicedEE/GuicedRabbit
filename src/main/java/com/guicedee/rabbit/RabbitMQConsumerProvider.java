package com.guicedee.rabbit;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.guicedee.client.IGuiceContext;
import io.vertx.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQConsumer;
import jakarta.inject.Singleton;
import lombok.extern.java.Log;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;

import static com.guicedee.rabbit.implementations.RabbitPostStartup.toOptions;

@Log
@Singleton
public class RabbitMQConsumerProvider implements Provider<QueueConsumer>
{
    @Inject
    private RabbitMQClient client;

    private final QueueDefinition queueDefinition;
    private final Class<? extends QueueConsumer> clazz;

    private QueueConsumer queueConsumer = null;
    private RabbitMQConsumer consumer = null;

    CompletableFuture<Void> future = new CompletableFuture<>().newIncompleteFuture();
    public static List<CompletableFuture<Void>> consumerCreated = new ArrayList<>();

    public RabbitMQConsumerProvider(QueueDefinition queueDefinition, Class<? extends QueueConsumer> clazz)
    {
        this.queueDefinition = queueDefinition;
        this.clazz = clazz;
    }

    @Override
    public QueueConsumer get()
    {
        if (!client.isConnected())
        {
            client.addConnectionEstablishedCallback((connectionEstablished) -> {
                if (queueConsumer != null)
                {
                    createConsumer();
                }
            });
        }
        else
        {
            createConsumer();
        }
        CompletableFuture.allOf(future).join();
        return queueConsumer;
    }

    private void createConsumer()
    {
        consumerCreated.add(future);
        client.basicConsumer(queueDefinition.value(), toOptions(this.queueDefinition.options()), (event) -> {
            if (event.succeeded())
            {
                consumer = event.result();
                consumer.setQueueName(queueDefinition.value());
                consumer = consumer.fetch(queueDefinition.options()
                                                         .fetchCount());
                consumer.handler((message) -> {
                    try
                    {
                        try
                        {
                            if (queueConsumer == null)
                            {
                                queueConsumer = clazz.newInstance();
                                IGuiceContext.instance()
                                             .inject()
                                             .injectMembers(queueConsumer);
                            }
                        }
                        catch (InstantiationException | IllegalAccessException e)
                        {
                            throw new RuntimeException(e);
                        }
                        queueConsumer.consume(message);
                    }
                    catch (Throwable e)
                    {
                        throw new RuntimeException(e);
                    }
                });
                future.complete(null);
            }
            else
            {
                log.log(Level.SEVERE, "Could not bind rabbit mq consumer on queue [" + queueDefinition.value() + "]", event.cause());
            }
        });
    }

    public void pause()
    {
        consumer.pause();
    }

    public void resume()
    {
        consumer.resume();
    }
}
