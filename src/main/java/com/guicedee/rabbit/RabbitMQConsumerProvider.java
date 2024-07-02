package com.guicedee.rabbit;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.guicedee.client.CallScoper;
import com.guicedee.client.IGuiceContext;
import com.guicedee.guicedinjection.interfaces.IGuicePreDestroy;
import com.guicedee.guicedservlets.websockets.options.CallScopeProperties;
import com.guicedee.guicedservlets.websockets.options.CallScopeSource;
import com.guicedee.rabbit.support.TransactedMessageConsumer;
import io.vertx.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQConsumer;
import lombok.extern.java.Log;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;

import static com.guicedee.rabbit.implementations.RabbitPostStartup.toOptions;

@Log
public class RabbitMQConsumerProvider implements Provider<QueueConsumer>, IGuicePreDestroy<RabbitMQConsumerProvider>
{
    @Inject
    private RabbitMQClient client;

    private final QueueDefinition queueDefinition;
    private final Class<? extends QueueConsumer> clazz;

    private QueueConsumer queueConsumer = null;
    private RabbitMQConsumer consumer = null;

    private String routingKey;
    private String exchange;

    CompletableFuture<Void> future = new CompletableFuture<>().newIncompleteFuture();
    public static List<CompletableFuture<Void>> consumerCreated = new ArrayList<>();

    public RabbitMQConsumerProvider(QueueDefinition queueDefinition, Class<? extends QueueConsumer> clazz, String routingKey, String exchange)
    {
        this.queueDefinition = queueDefinition;
        this.clazz = clazz;
        this.routingKey = routingKey;
        this.exchange = exchange;
    }

    @Inject
    void setup()
    {
        if (queueDefinition.options()
                           .autobind())
        {
            log.config("Setting up queue consumer - " + clazz.getSimpleName() + " - " + queueDefinition.value());
            buildConsumer();
        }
        else
        {
            log.warning("Queue consumer not being created on definition. To consume queue make sure to call binding");
        }
    }

    private void buildConsumer()
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

    @Override
    public QueueConsumer get()
    {
        if (queueConsumer == null)
        {
            buildConsumer();
        }
        CompletableFuture.allOf(future)
                         .join();
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
                        IGuiceContext.instance().getLoadingFinished().thenRunAsync(() -> {
                            CallScoper scoper = IGuiceContext.get(CallScoper.class);
                            scoper.enter();
                            CallScopeProperties properties = IGuiceContext.get(CallScopeProperties.class);
                            properties.setSource(CallScopeSource.RabbitMQ);
                            try
                            {
                                if (queueDefinition.options()
                                                   .transacted())
                                {
                                    TransactedMessageConsumer transactedMessageConsumer = IGuiceContext.get(TransactedMessageConsumer.class);
                                    if (queueConsumer != null)
                                    {
                                        transactedMessageConsumer.setQueueConsumer(queueConsumer);
                                    }
                                    try
                                    {
                                        transactedMessageConsumer.execute(clazz, message);
                                        if (!queueDefinition.options()
                                                            .autoAck())
                                        {
                                            client.basicAck(message.envelope()
                                                                   .getDeliveryTag(), false, asyncResult -> {
                                                log.fine("Confirmed send of message");
                                            });
                                        }
                                    }catch (Throwable T)
                                    {
                                        if (!queueDefinition.options()
                                                            .autoAck())
                                        {
                                            client.basicNack(message.envelope()
                                                                    .getDeliveryTag(), false,false, asyncResult -> {
                                            });
                                        }
                                        log.log(Level.SEVERE,"ERROR processing of transacted message",T);
                                    }
                                    if (queueConsumer == null)
                                    {
                                        queueConsumer = transactedMessageConsumer.getQueueConsumer();
                                    }
                                }
                                else
                                {
                                    if (queueConsumer == null)
                                    {
                                        queueConsumer = IGuiceContext.get(clazz);
                                    }
                                    try
                                    {
                                        queueConsumer.consume(message);
                                        if (!queueDefinition.options()
                                                            .autoAck())
                                        {
                                            client.basicAck(message.envelope()
                                                                   .getDeliveryTag(), false, asyncResult -> {
                                                log.fine("Confirmed send of message");
                                            });

                                        }
                                    }
                                    catch (Throwable e)
                                    {
                                        log.log(Level.SEVERE, "Error while creating consumer - " + clazz.getSimpleName(), e);
                                        if (!queueDefinition.options()
                                                            .autoAck())
                                        {
                                            client.basicNack(message.envelope()
                                                                   .getDeliveryTag(), false,false, asyncResult -> {
                                                log.log(Level.SEVERE,"ERROR send of message",e);
                                            });
                                        }
                                        throw new RuntimeException(e);
                                    }
                                }
                            }
                            finally
                            {
                                scoper.exit();
                            }
                        });
                    }
                    catch (Throwable e)
                    {
                        log.log(Level.SEVERE, "Error while creating consumer", e);
                        throw new RuntimeException(e);
                    }
                });
            }
            else
            {
                log.log(Level.SEVERE, "Could not bind rabbit mq consumer on queue [" + queueDefinition.value() + "]", event.cause());
            }
            future.complete(null);
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

    @Override
    public void onDestroy()
    {
        if (consumer != null)
        {
            log.config("Shutting down consumer - " + clazz.getSimpleName());
            consumer.cancel();
        }
    }

    @Override
    public Integer sortOrder()
    {
        return 25;
    }
}
