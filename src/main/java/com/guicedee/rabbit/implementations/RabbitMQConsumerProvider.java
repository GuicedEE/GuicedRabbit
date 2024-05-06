package com.guicedee.rabbit.implementations;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.guicedee.client.IGuiceContext;
import com.guicedee.rabbit.QueueConsumer;
import com.guicedee.rabbit.QueueDefinition;
import io.vertx.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQConsumer;
import jakarta.inject.Singleton;
import lombok.extern.java.Log;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static com.guicedee.rabbit.QueuePublisher.done;
import static com.guicedee.rabbit.implementations.RabbitPostStartup.toOptions;

@Log
@Singleton
public class RabbitMQConsumerProvider implements Provider<QueueConsumer>
{
    @Inject
    private RabbitMQClient client;

    private final QueueDefinition queueDefinition;
    private final Class<QueueConsumer> clazz;

    private QueueConsumer queueConsumer = null;
    private RabbitMQConsumer consumer = null;
    private AtomicBoolean created = new AtomicBoolean(false);

    CompletableFuture exchangeFuture = null;

    @SuppressWarnings("unchecked")
    public RabbitMQConsumerProvider(QueueDefinition queueDefinition, Class clazz)
    {
        this.queueDefinition = queueDefinition;
        this.clazz = clazz;
    }

    @Override
    public QueueConsumer get()
    {
        done.thenRun(() -> {
            if (queueConsumer == null || consumer.isCancelled())
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
            }
        });
        return queueConsumer;
    }

    private void createConsumer()
    {

        if (!created.get())
        {
            created.set(true);
            client.basicConsumer(queueDefinition.value(), toOptions(this.queueDefinition.options()), (event) -> {
                if (event.succeeded())
                {
                    consumer = event.result();
                    consumer.setQueueName(queueDefinition.value());
                    consumer = consumer.fetch(queueDefinition.options()
                                                             .fetchCount());
                    queueConsumer = IGuiceContext.get(clazz);
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
                    consumer.handler((message) -> {
                        try
                        {
                            queueConsumer.consume(message);
                        }
                        catch (Throwable e)
                        {
                            throw new RuntimeException(e);
                        }
                    });
                }
                else
                {
                    log.log(Level.SEVERE, "Could not bind rabbit mq consumer on queue [" + queueDefinition.value() + "]", event.cause());
                    created.set(false);
                }
            });
        }
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
