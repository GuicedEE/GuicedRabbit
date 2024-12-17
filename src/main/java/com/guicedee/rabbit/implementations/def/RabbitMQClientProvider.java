package com.guicedee.rabbit.implementations.def;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.guicedee.client.Environment;
import com.guicedee.client.IGuiceContext;
import com.guicedee.guicedinjection.interfaces.IGuicePreDestroy;
import com.guicedee.rabbit.QueueDefinition;
import com.guicedee.rabbit.QueueExchange;
import com.guicedee.rabbit.RabbitConnectionOptions;
import com.guicedee.rabbit.implementations.RabbitMQModule;
import com.guicedee.vertx.spi.VertXPreStartup;
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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.java.Log;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;

@Log
@EqualsAndHashCode(of = {"providerId"})
public class RabbitMQClientProvider implements Provider<RabbitMQClient>,
        IGuicePreDestroy<RabbitMQClientProvider> {
    private UUID providerId = UUID.randomUUID();

    @Inject
    Vertx vertx;

    private final RabbitMQOptions options;
    private final RabbitConnectionOptions connectionOptions;
    private final String connectionPackage;

    @Getter
    private final CompletableFuture<Boolean> exchangeDeclared = new CompletableFuture<>().newIncompleteFuture();

    // public static Future<Void> startQueueFuture;

    //public static CompletableFuture<Void> rabbitMQClientStarted = new CompletableFuture<>().newIncompleteFuture();
    //public static List<CompletableFuture<Void>> queueBindingFutures = new ArrayList<>();

    @Getter
    private RabbitMQClient client;

    public RabbitMQClientProvider(RabbitMQOptions options, RabbitConnectionOptions connectionOptions, String connectionPackage) {
        this.options = options;
        this.connectionOptions = connectionOptions;
        this.connectionPackage = connectionPackage;
    }

    private void handle(RabbitMQClient rabbitMQClient, AsyncResult<Void> asyncResult) {
        if (asyncResult.succeeded()) {
            log.config("RabbitMQ successfully connected!");
            configure(rabbitMQClient);
        } else {
            log.log(Level.SEVERE, "Fail to connect to RabbitMQ " + asyncResult.cause()
                    .getMessage(), asyncResult.cause());
        }
    }


    @Override
    public RabbitMQClient get() {
        if (client != null) {
            return this.client;
        }
        if (vertx == null) {
            vertx = VertXPreStartup.getVertx();
        }
        if (!"5672".equalsIgnoreCase(Environment.getProperty("RABBIT_MQ_PORT", "5672"))) {
            options.setPort(Integer.parseInt(Environment.getProperty("RABBIT_MQ_PORT", "5672")));
        }
        client = RabbitMQClient.create(vertx, options);
        configure(client);
       /* startQueueFuture = client.start();
        //    startQueueFuture.andThen((result) -> handle(client, result))
        //                    .result();
        try {
            rabbitMQClientStarted.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }*/
        return client;
    }

    private void configure(RabbitMQClient rabbitMQClient) {
        ScanResult scanResult = IGuiceContext.instance()
                .getScanResult();
        var pi = scanResult.getPackageInfo(connectionPackage);
        ClassInfoList queueConsumers = pi.getClassInfoRecursive().filter(a -> a.hasAnnotation(QueueDefinition.class));
        //.getClassesWithAnnotation(QueueDefinition.class);

        ClassInfoList exchangeAnnotations = pi.getClassInfoRecursive().filter(a -> a.hasAnnotation(QueueExchange.class));

        if (!rabbitMQClient.isConnected()) {
            rabbitMQClient.addConnectionEstablishedCallback(promise -> {
                processNewConnection(rabbitMQClient, exchangeAnnotations, queueConsumers);
            });
        } else {
            processNewConnection(rabbitMQClient, exchangeAnnotations, queueConsumers);
        }
    }

    private static Set<String> exchanges = new HashSet<>();

    private void processNewConnection(RabbitMQClient rabbitMQClient, ClassInfoList exchangeAnnotations, ClassInfoList queueConsumers) {

        for (ClassInfo exchangeAnnotation : exchangeAnnotations) {
            var queueExchange = exchangeAnnotation.loadClass().getAnnotation(QueueExchange.class);
            String exchangeName = queueExchange.value();
            String deadLetter = exchangeName + ".deadletter";

            if (this.exchanges.contains(exchangeName)) {
                return;
            } else this.exchanges.add(exchangeName);

            if (connectionOptions.confirmPublishes()) {
                client.confirmSelect()
                        .onComplete(confirmResult -> {
                            if (confirmResult.succeeded()) {
                                log.config("Configured Publisher Confirmation on connection [" + connectionOptions.value() + "]");
                            } else {
                                log.log(Level.WARNING, "Configured Publisher Confirmation on connection [" + connectionOptions.value() + "]", confirmResult.cause());
                            }
                        });
            }

            if (queueExchange.createDeadLetter()) {
                JsonObject config = new JsonObject();
                config.put("x-dead-letter-exchange", deadLetter);
                //declare dead letter
                rabbitMQClient.exchangeDeclare(deadLetter, queueExchange.exchangeType()
                                .toString(), queueExchange.durable(), queueExchange.autoDelete())
                        .onComplete(onResult -> {
                            if (onResult.succeeded()) {

                                log.config("Dead Letter Exchange successfully declared - " + deadLetter);
                                config.put("alternate-exchange", exchangeName);
                                rabbitMQClient.exchangeDeclare(exchangeName, queueExchange.exchangeType()
                                                .toString(), queueExchange.durable(),
                                        queueExchange.autoDelete(),
                                        config
                                ).onComplete(exchangeDeclared -> {
                                    if (exchangeDeclared.succeeded()) {
                                        log.info("Exchange [" + exchangeName + "] successfully declared with Dead Letter Exchange [" + deadLetter + "]");
                                        Set<OnQueueExchangeDeclared> onQueueExchange = IGuiceContext.loaderToSetNoInjection(ServiceLoader.load(OnQueueExchangeDeclared.class));
                                        onQueueExchange.forEach(a -> a.perform(client, exchangeName));
                                        //   createQueue(rabbitMQClient, queueConsumers, exchangeName);

                                        var exchangeClient = RabbitMQModule.exchangeClients.get(exchangeName);
                                        RabbitMQModule.queueExchangeNames.forEach((queue, queueExchangeName) -> {
                                            if (exchangeName.equalsIgnoreCase(queueExchangeName)) {
                                                log.info("Binding queue [" + queue + "] to exchange [" + queueExchangeName + "]");
                                                var qc = RabbitMQModule.queueConsumers.get(queue);
                                                if (qc != null) {
                                                    //  exchangeClient.exchangeDeclared.whenCompleteAsync((result, error) -> {
                                                    log.info("Creating Queue - " + queue);
                                                    qc.createQueue(exchangeClient.get(), null, exchangeName);
                                                    // });
                                                } else {
                                                    log.severe("Queue was not found in the consumers list - " + queue);
                                                }
                                            }
                                        });
                                 /*       RabbitMQModule.queueConsumers
                                                .forEach((queue, consumer) -> {
                                                    if(consumer.getQueueDefinition())
                                                    consumer.createQueue(exchangeClient.get(), null, exchangeName);
                                                });*/

                                    } else {
                                        log.log(Level.SEVERE, "Cannot create exchange ", exchangeDeclared.cause());
                                    }
                                    //        rabbitMQClientStarted.complete(null);
                                });
                                exchangeDeclared.complete(true);
                            } else {
                                log.log(Level.SEVERE, "Cannot create dead letter queue", onResult.cause());
                                exchangeDeclared.complete(false);
                            }

                        });
            } else {
                rabbitMQClient.exchangeDeclare(exchangeName, queueExchange.exchangeType()
                                .toString(), queueExchange.durable(), queueExchange.autoDelete())
                        .onComplete(exchangeDeclared -> {
                            if (exchangeDeclared.succeeded()) {
                                log.info("Exchange successfully declared with config - " + exchangeName);
                                Set<OnQueueExchangeDeclared> onQueueExchange = IGuiceContext.loaderToSetNoInjection(ServiceLoader.load(OnQueueExchangeDeclared.class));
                                onQueueExchange.forEach(a -> a.perform(client, exchangeName));
                                this.exchangeDeclared.complete(true);
                                // createQueue(rabbitMQClient, queueConsumers, exchangeName);
                            } else {
                                log.log(Level.SEVERE, "Cannot create exchange ", exchangeDeclared.cause());
                                this.exchangeDeclared.complete(false);
                            }
                            //   rabbitMQClientStarted.complete(null);
                        });
            }
        }

    }


    @Override
    public void onDestroy() {
        if (client != null && client.isConnected()) {
            client.stop().onComplete((a) -> {
                log.config("Rabbit MQ Client Shutdown");
            });
        }
    }

    //after consumers shutdown
    @Override
    public Integer sortOrder() {
        return 50;
    }
}
