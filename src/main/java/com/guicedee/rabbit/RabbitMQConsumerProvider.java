package com.guicedee.rabbit;

import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.name.Names;
import com.guicedee.client.CallScoper;
import com.guicedee.client.IGuiceContext;
import com.guicedee.guicedinjection.interfaces.IGuicePreDestroy;
import com.guicedee.guicedservlets.websockets.options.CallScopeProperties;
import com.guicedee.guicedservlets.websockets.options.CallScopeSource;
import com.guicedee.rabbit.implementations.def.RabbitMQClientProvider;
import com.guicedee.rabbit.support.TransactedMessageConsumer;
import io.github.classgraph.ClassInfoList;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonObject;
import io.vertx.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQConsumer;
import lombok.Getter;
import lombok.extern.java.Log;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.guicedee.rabbit.implementations.RabbitPostStartup.toOptions;

@Log
public class RabbitMQConsumerProvider implements Provider<QueueConsumer>, IGuicePreDestroy<RabbitMQConsumerProvider>
{
    private RabbitMQClientProvider client;

    @Getter
    private final QueueDefinition queueDefinition;
    @Getter
    private final Class<? extends QueueConsumer> clazz;

    private QueueConsumer queueConsumer = null;
    private RabbitMQConsumer consumer = null;

    private String routingKey;
    private String exchange;
    private final CompletableFuture<Boolean> exchangeFuture;

    @Inject
    private Vertx vertx;

    private WorkerExecutor workerExecutor;

    //CompletableFuture<Void> future = new CompletableFuture<>().newIncompleteFuture();
    //public static List<CompletableFuture<Void>> consumerCreated = new ArrayList<>();

    @Inject
    void setup()
    {
        workerExecutor = vertx.createSharedWorkerExecutor("rabbit-consumer-worker-thread");
    }

    public RabbitMQConsumerProvider(RabbitMQClientProvider client, QueueDefinition queueDefinition, Class<? extends QueueConsumer> clazz,
                                    String routingKey, String exchange, CompletableFuture<Boolean> exchangeFuture)
    {
        this.client = client;
        this.queueDefinition = queueDefinition;
        this.clazz = clazz;
        this.routingKey = routingKey;
        this.exchange = exchange;
        this.exchangeFuture = exchangeFuture;

        // this.client.get().addConnectionEstablishedCallback((connectionEstablished) -> {
        exchangeFuture.thenAccept((result) -> {
            workerExecutor.executeBlocking(()->{
                createQueue(client.get(), null, this.exchange);
                return null;
            });
        });
    }

    public void createQueue(RabbitMQClient rabbitMQClient, ClassInfoList queueConsumers, String exchangeName)
    {

        QueueDefinition queueDefinition = clazz
                .getAnnotation(QueueDefinition.class);
        if (queueDefinition == null)
        {
            throw new RuntimeException("Queue definition not found for queue class - " + clazz.getName());
        }
        JsonObject queueConfig = new JsonObject();
        if (queueDefinition.options()
                .ttl() != 0)
        {
            queueConfig.put("x-message-ttl", queueDefinition.options()
                    .ttl() + "");
        }
        if (queueDefinition.options()
                .singleConsumer())
        {
            queueConfig.put("x-single-active-consumer", true);
        }
        if (queueDefinition.options()
                .priority() != 0)
        {
            queueConfig.put("x-max-priority", queueDefinition.options().priority());
        }
        rabbitMQClient.queueDeclare(queueDefinition.value(),
                queueDefinition.options()
                        .durable(),
                queueDefinition.options()
                        .consumerExclusive(),
                queueDefinition.options()
                        .delete(),
                queueConfig
        ).onComplete(result -> {
            if (result.succeeded())
            {
                //    String routingKey = exchangeName + "_" + queueDefinition.value();
                Map<String, Object> arguments = new HashMap<>();
                //CompletableFuture<Void> completableFuture = new CompletableFuture<>().newIncompleteFuture();
                //    queueBindingFutures.add(completableFuture);
                //then bind the queue
                rabbitMQClient.queueBind(queueDefinition.value(), exchangeName, routingKey, arguments)
                        .onComplete(onResult -> {
                            if (onResult.succeeded())
                            {
                                log.config("Bound queue [" + queueDefinition.value() + "] successfully");
                                if (queueConsumer == null)
                                {
                                    createConsumer();
                                }
                            } else
                            {
                                log.log(Level.SEVERE, "Cannot bind queue ", onResult.cause());
                            }
                            //    completableFuture.complete(null);
                        });
            } else
            {
                log.log(Level.SEVERE, "Cannot bind queue ", result.cause());
            }
        });

    }


    private void buildConsumer()
    {
        if (!client.get().isConnected())
        {
            client.get().addConnectionEstablishedCallback((connectionEstablished) -> {
                if (queueConsumer != null)
                {
                    createConsumer();
                }
            });
        } else
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
        return queueConsumer;
    }

//private boolean consumersCreated = false;

    private static Set<String> consumersCreated = new HashSet<>();

    private void createConsumer()
    {
        for (int i = 0; i < this.queueDefinition.options().consumerCount(); i++)
        {
            var opts = toOptions(this.queueDefinition.options());
            opts.setConsumerTag(this.routingKey + "_consumer" + (i + 1));

            if (consumersCreated.contains(opts.getConsumerTag()))
            {
                continue;
            } else
            {
                consumersCreated.add(opts.getConsumerTag());
            }

            client.get().basicConsumer(queueDefinition.value(),
                            opts)
                    .onComplete((event) -> {
                        if (event.succeeded())
                        {
                            consumer = event.result();
                            consumer.setQueueName(queueDefinition.value());
                            consumer = consumer.fetch(queueDefinition.options()
                                    .fetchCount());

                            if (queueDefinition.options()
                                    .transacted())
                            {
                                IGuiceContext.instance().getLoadingFinished().onSuccess((handler) -> {
                                    TransactedMessageConsumer tmc = IGuiceContext.get(Key.get(TransactedMessageConsumer.class, Names.named(queueDefinition.value())));
                                    tmc.setQueueDefinition(queueDefinition);
                                    tmc.setClazz(clazz);
                                });
                            }

                            consumer.handler((message) -> {
                                try
                                {
                                    //  IGuiceContext.instance().getLoadingFinished().thenRunAsync(() -> {
                                    // CompletableFuture.allOf(consumerCreated.toArray(new CompletableFuture[0])).whenComplete((a, e) -> {
                                    CallScoper scoper = IGuiceContext.get(CallScoper.class);
                                    scoper.enter();
                                    CallScopeProperties properties = IGuiceContext.get(CallScopeProperties.class);
                                    properties.setSource(CallScopeSource.RabbitMQ);
                                    try
                                    {
                                        if (queueDefinition.options()
                                                .transacted())
                                        {
                                            workerExecutor.executeBlocking(() -> {
                                                TransactedMessageConsumer transactedMessageConsumer = IGuiceContext.get(Key.get(TransactedMessageConsumer.class, Names.named(queueDefinition.value())));
                                                transactedMessageConsumer.setQueueDefinition(queueDefinition);
                                                //var q = CompletableFuture.runAsync(() ->
                                                {
                                                    CallScoper scopeRunner = IGuiceContext.get(CallScoper.class);
                                                    scopeRunner.enter();
                                                    try
                                                    {
                                                        log.config("Running transacted message consumer - " + queueDefinition.value());
                                                        IGuiceContext.get(CallScopeProperties.class)
                                                                .setProperties(properties.getProperties())
                                                                .setSource(properties.getSource());
                                                        try
                                                        {
                                                            transactedMessageConsumer.execute(clazz, message);
                                                            if (!queueDefinition.options()
                                                                    .autoAck())
                                                            {
                                                                client.get().basicAck(message.envelope()
                                                                                .getDeliveryTag(), false)
                                                                        .onComplete(asyncResult -> {
                                                                            if (asyncResult.succeeded())
                                                                            {
                                                                                log.fine("Message Acknowledged Successfully");
                                                                            } else
                                                                            {
                                                                                log.log(Level.SEVERE, "Message Acknowledged Failed", asyncResult.cause());
                                                                            }
                                                                        });
                                                            }
                                                        } catch (Throwable T)
                                                        {
                                                            if (!queueDefinition.options()
                                                                    .autoAck())
                                                            {
                                                                client.get().basicNack(message.envelope()
                                                                                .getDeliveryTag(), false, false)
                                                                        .onComplete(asyncResult -> {
                                                                            if (asyncResult.succeeded())
                                                                            {
                                                                                log.warning("Message NAcknowledged Successfully");
                                                                            } else
                                                                            {
                                                                                log.log(Level.SEVERE, "Message NAcknowledged Failed", asyncResult.cause());
                                                                            }
                                                                        });
                                                            }
                                                            log.log(Level.SEVERE, "ERROR processing of transacted message", T);
                                                        }
                                                    } finally
                                                    {
                                                        scopeRunner.exit();
                                                    }

                                                }
                                                if (queueDefinition.options().singleConsumer())
                                                {
                                                    try
                                                    {
                                                        log.info("Processing Synchronous - " + queueDefinition.value());
                                                        //     q.get(30, TimeUnit.SECONDS);
                                                    } catch (Exception er)
                                                    {
                                                        log.log(Level.SEVERE, "Operation Timed Out - " + queueDefinition.value(), er);
                                                    }
                                                }
                                                if (queueConsumer == null)
                                                {
                                                    queueConsumer = transactedMessageConsumer.getQueueConsumer();
                                                }
                                                return null;
                                            },queueDefinition.options().singleConsumer());
                                        } else
                                        {
                                            workerExecutor.executeBlocking(() -> {
                                                if (queueConsumer == null)
                                                {
                                                    queueConsumer = IGuiceContext.get(clazz);
                                                }
                                                try
                                                {
                                                    {
                                                        CallScoper scopeRunner = IGuiceContext.get(CallScoper.class);
                                                        scopeRunner.enter();
                                                        try
                                                        {
                                                            IGuiceContext.get(CallScopeProperties.class)
                                                                    .setProperties(properties.getProperties())
                                                                    .setSource(properties.getSource());
                                                            queueConsumer.consume(message);
                                                            if (!queueDefinition.options()
                                                                    .autoAck())
                                                            {
                                                                client.get().basicAck(message.envelope()
                                                                                .getDeliveryTag(), false)
                                                                        .onComplete(asyncResult -> {
                                                                            if (asyncResult.succeeded())
                                                                            {
                                                                                log.fine("Message Acknowledged Successfully");
                                                                            } else
                                                                            {
                                                                                log.log(Level.SEVERE, "Message Acknowledged Failed", asyncResult.cause());
                                                                            }
                                                                        });

                                                            }
                                                        } finally
                                                        {
                                                            scopeRunner.exit();
                                                        }
                                                    }
                                                    ;
                                                    if (queueDefinition.options().singleConsumer())
                                                    {
                                                        try
                                                        {
                                                           // log.info("Processing Synchronous - " + queueDefinition.value());
                                                            // q.get(30, TimeUnit.SECONDS);
                                                        } catch (Exception er)
                                                        {
                                                            log.log(Level.SEVERE, "Operation Timed Out - " + queueDefinition.value(), er);
                                                        }
                                                    }
                                                } catch (Throwable e2)
                                                {
                                                    log.log(Level.SEVERE, "Error while creating consumer - " + clazz.getSimpleName(), e2);
                                                    if (!queueDefinition.options()
                                                            .autoAck())
                                                    {
                                                        workerExecutor.executeBlocking(() -> {
                                                            client.get().basicNack(message.envelope()
                                                                            .getDeliveryTag(), false, false)
                                                                    .onComplete(asyncResult -> {
                                                                        if (asyncResult.succeeded())
                                                                        {
                                                                            log.fine("Message Acknowledged Successfully");
                                                                        } else
                                                                        {
                                                                            log.log(Level.SEVERE, "Message Acknowledged Failed", asyncResult.cause());
                                                                        }
                                                                    });
                                                            return null;
                                                        },false);
                                                    }
                                                    throw new RuntimeException(e2);
                                                }
                                                return null;
                                            },queueDefinition.options().singleConsumer());
                                        }
                                    } finally
                                    {
                                        scoper.exit();
                                    }
                                    //  });
                                    //  });
                                } catch (Throwable e)
                                {
                                    log.log(Level.SEVERE, "Error while creating consumer", e);
                                    throw new RuntimeException(e);
                                }
                            });
                            //  future.complete(null);
                        } else
                        {
                            log.log(Level.SEVERE, "Could not bind rabbit mq consumer on queue [" + queueDefinition.value() + "]", event.cause());
                            //   future.complete(null);
                        }

                    });
        } // end of consumer count loop
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
