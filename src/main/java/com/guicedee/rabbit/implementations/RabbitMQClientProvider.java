package com.guicedee.rabbit.implementations;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.guicedee.client.IGuiceContext;
import com.guicedee.rabbit.QueueDefinition;
import com.guicedee.rabbit.QueueExchange;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQOptions;
import jakarta.inject.Singleton;
import lombok.extern.java.Log;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

@Singleton
@Log
public class RabbitMQClientProvider extends AbstractVerticle implements Provider<RabbitMQClient>
{
    @Inject
    Vertx vertx;

    private final RabbitMQOptions options;
    public static Future<Void> startQueueFuture;

    public RabbitMQClientProvider(RabbitMQOptions options)
    {
        this.options = options;
    }

    private static void handle(RabbitMQClient rabbitMQClient, AsyncResult<Void> asyncResult)
    {
        if (asyncResult.succeeded())
        {
            log.config("RabbitMQ successfully connected!");
            configure(rabbitMQClient);
        }
        else
        {
            log.log(Level.SEVERE, "Fail to connect to RabbitMQ " + asyncResult.cause()
                                                                              .getMessage(), asyncResult.cause());
        }
    }

    @Override
    public RabbitMQClient get()
    {
        RabbitMQClient client = RabbitMQClient.create(vertx, options);
        startQueueFuture = client.start();
        startQueueFuture.andThen((result) -> handle(client, result));
        while(!startQueueFuture.isComplete())
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
        return client;
    }

    @Override
    public void start() throws Exception
    {
        super.start();
    }

    private static void configure(RabbitMQClient rabbitMQClient)
    {
        ScanResult scanResult = IGuiceContext.instance()
                                             .getScanResult();
        ClassInfoList queueConsumers = scanResult
                .getClassesWithAnnotation(QueueDefinition.class);
        ClassInfoList exchangeAnnotations = scanResult.getClassesWithAnnotation(QueueExchange.class);

        rabbitMQClient.addConnectionEstablishedCallback(promise -> {

            for (ClassInfo exchanges : exchangeAnnotations)
            {
                QueueExchange queueExchange = exchanges.loadClass()
                                                       .getAnnotation(QueueExchange.class);
                String exchangeName = queueExchange.value();
                String deadLetter = exchangeName + ".deadletter";
                if (queueExchange.createDeadLetter())
                {
                    JsonObject config = new JsonObject();
                    config.put("x-dead-letter-exchange", deadLetter);
                    //declare dead letter
                    rabbitMQClient.exchangeDeclare(deadLetter, queueExchange.exchangeType()
                                                                            .toString(), queueExchange.durable(), queueExchange.autoDelete(), onResult -> {
                        if (onResult.succeeded())
                        {
                            log.config("Dead Letter Exchange successfully declared ");
                            config.put("alternate-exchange", exchangeName);
                            rabbitMQClient.exchangeDeclare(exchangeName, queueExchange.exchangeType()
                                                                                      .toString(), queueExchange.durable(),
                                                           queueExchange.autoDelete(),
                                                           config,
                                                           exchangeDeclared -> {
                                                               if (exchangeDeclared.succeeded())
                                                               {
                                                                   log.info("Exchange successfully declared with Dead Letter Exchange");
                                                                   createQueue(rabbitMQClient, queueConsumers, exchangeName);
                                                               }
                                                               else
                                                               {
                                                                   log.log(Level.SEVERE, "Cannot create exchange ", exchangeDeclared.cause());
                                                               }
                                                           });
                        }
                        else
                        {
                            log.log(Level.SEVERE, "Cannot create dead letter queue", onResult.cause());
                        }
                    });
                }
                else
                {
                    rabbitMQClient.exchangeDeclare(exchangeName, queueExchange.exchangeType()
                                                                              .toString(), queueExchange.durable(), queueExchange.autoDelete(), onResult -> {
                        if (onResult.succeeded())
                        {
                            log.info("Exchange successfully declared with config");
                            createQueue(rabbitMQClient, queueConsumers, exchangeName);
                        }
                        else
                        {
                            log.log(Level.SEVERE, "Cannot create exchange ", onResult.cause());
                        }
                    });
                }

            }
        });
    }

    private static void createQueue(RabbitMQClient rabbitMQClient, ClassInfoList queueConsumers, String exchangeName)
    {
        for (ClassInfo consumer : queueConsumers)
        {
            QueueDefinition queueDefinition = consumer.loadClass()
                                                      .getAnnotation(QueueDefinition.class);
            if (queueDefinition == null)
            {
                continue;
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
            rabbitMQClient.queueDeclare(queueDefinition.value(),
                                        queueDefinition.options()
                                                       .durable(),
                                        queueDefinition.options()
                                                       .consumerExclusive(),
                                        queueDefinition.options()
                                                       .delete(),
                                        queueConfig,
                                        result -> {
                                            if (result.succeeded())
                                            {
                                                String routingKey = exchangeName + "_" + queueDefinition.value();
                                                Map<String, Object> arguments = new HashMap<>();

                                                //then bind the queue
                                                rabbitMQClient.queueBind(queueDefinition.value(), exchangeName, routingKey, arguments, onResult -> {
                                                    if (onResult.succeeded())
                                                    {
                                                        log.info("Bound queue [" + queueDefinition.value() + "] successfully");
                                                    }
                                                    else
                                                    {
                                                        log.log(Level.SEVERE, "Cannot bind queue ", onResult.cause());
                                                    }
                                                });
                                            }
                                        }
            );
        }
    }
}
