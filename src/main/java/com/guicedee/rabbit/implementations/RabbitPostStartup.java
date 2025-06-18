package com.guicedee.rabbit.implementations;

import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.name.Names;
import com.guicedee.client.CallScoper;
import com.guicedee.client.IGuiceContext;
import com.guicedee.guicedinjection.interfaces.IGuicePostStartup;
import com.guicedee.client.CallScopeProperties;
import com.guicedee.client.CallScopeSource;
import com.guicedee.rabbit.QueueConsumer;
import com.guicedee.rabbit.QueueDefinition;
import com.guicedee.rabbit.QueueExchange;
import com.guicedee.rabbit.RabbitConnectionOptions;
import com.guicedee.rabbit.support.TransactedMessageConsumer;
import com.guicedee.vertx.spi.VertXPreStartup;
import com.rabbitmq.client.AMQP;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.rabbitmq.QueueOptions;
import io.vertx.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQConsumer;
import io.vertx.rabbitmq.RabbitMQMessage;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Log4j2
public class RabbitPostStartup implements IGuicePostStartup<RabbitPostStartup>
{

    @Inject
    private Vertx vertx;

    @Override
    public List<Future<Boolean>> postLoad()
    {
        RabbitMQPreStartup.getPackageRabbitMqConnections().forEach((packageName, connections) -> {
            for (RabbitConnectionOptions connectionOption : connections)
            {
                var client = RabbitMQModule.getPackageClients().get(packageName);
                boolean confirmPublishSet = connectionOption.confirmPublishes();
                RabbitMQPreStartup.getPackageExchanges()
                        .keySet()
                        .stream()
                        .filter(a -> a.startsWith(packageName))
                        .forEach(exchangePackage -> {

                            var exchangeName = RabbitMQPreStartup.getPackageExchanges().get(exchangePackage);
                            if (!RabbitMQModule.getExchangeClients().containsKey(exchangeName))
                                RabbitMQModule.getExchangeClients().put(exchangeName, client);

                            var exchangeNameProperties = RabbitMQPreStartup.getExchangeDefinitions().get(exchangeName);
                            processExchangeRegistration(packageName, connectionOption, exchangeName, confirmPublishSet, client, exchangeNameProperties);
                        });
            }
        });
        return List.of(Future.succeededFuture(true));
    }

    private static final Map<String, Future<?>> exchangeFutures = new HashMap<>();

    private void processExchangeRegistration(String packageName, RabbitConnectionOptions connectionOption, String exchangeName, boolean confirmPublishSet, RabbitMQClient client, QueueExchange exchangeNameProperties)
    {
        RabbitMQModule.getPackageConnectionFutures().get(packageName)
                .onComplete(ar -> {
                    if (ar.succeeded())
                    {
                        if (confirmPublishSet)
                        {
                            client.confirmSelect().onComplete((result, error) -> {
                                if (error != null)
                                {
                                    log.error("Cannot set connection publishes watch - {} - {}", connectionOption.value(), error.getMessage());
                                } else
                                {
                                    log.info("Connection {} has confirm publishes enabled", connectionOption.value());
                                }
                            });
                        }
                        exchangeFutures.put(exchangeName, declareExchangeWithRetry(client, exchangeName,
                                exchangeNameProperties.exchangeType().toString(),
                                exchangeNameProperties.durable(),
                                exchangeNameProperties.autoDelete(),
                                exchangeNameProperties.createDeadLetter()));
                        exchangeFutures.get(exchangeName)
                                .onComplete(ar2 -> {
                                    //when exchange exists create consumer
                                    processExchangeConsumerQueues(client, exchangeName);
                                });
                    }
                });
    }


    private Future<Void> declareExchangeWithRetry(
            RabbitMQClient client,
            String exchangeName,
            String exchangeType,
            boolean durable,
            boolean autoDelete,
            boolean deadletter
    )
    {
        String dlxName = exchangeName + ".DLX"; // Dead Letter Exchange name

        JsonObject config = new JsonObject();
        if (deadletter)
            config.put("x-dead-letter-exchange", dlxName);
        // Declare the main exchange
        return client.exchangeDeclare(exchangeName, exchangeType, durable, autoDelete, config)
                .onSuccess(v -> log.info("Exchange '{}' declared successfully.", exchangeName))
                .compose(v -> {
                    if (deadletter)
                    {
                        // Declare Dead Letter Exchange
                        log.debug("Creating Dead Letter Exchange '{}'", dlxName);
                        return client.exchangeDeclare(dlxName, exchangeType, true, false) // DLX is durable and not auto-deleted
                                .onSuccess(dlxResult -> log.info("Dead Letter Exchange '{}' declared successfully.", dlxName))
                                .onFailure(dlxError -> log.error("Failed to declare Dead Letter Exchange '{}': {}", dlxName, dlxError.getMessage()));
                    }
                    return Future.succeededFuture();
                })
                .recover(declareError -> {
                    // Handle the failure if the initial exchange declaration fails
                    log.error("Failed to declare exchange '{}': {}", exchangeName, declareError.getMessage());
                    return client.exchangeDelete(exchangeName)
                            .compose(v -> {
                                log.info("Re-declaring exchange '{}'", exchangeName);
                                return client.exchangeDeclare(exchangeName, exchangeType, durable, autoDelete);
                            })
                            .onSuccess(v -> log.info("Exchange '{}' re-declared successfully.", exchangeName))
                            .onFailure(redeclareError -> {
                                log.error("Failed to re-declare exchange '{}': {}", exchangeName, redeclareError.getMessage());
                            });
                });
    }

    private Future<AMQP.Queue.DeclareOk> declareDeadLetterQueue(RabbitMQClient client, String dlxExchangeName, String queueName, boolean durable)
    {
        Map<String, Object> queueArguments = new HashMap<>();
        queueArguments.put("x-dead-letter-exchange", dlxExchangeName); // Bind queue to the DLX
        // queueArguments.put("x-message-ttl", 60000); // Optional: TTL example (1 minute)
        // queueArguments.put("x-max-length", 10000); // Optional: Limit max queue size (10,000 messages)

        String dlxQueueName = queueName + ".DLX"; // Name of the queue bound to the DLX
        return client.queueDeclare(dlxQueueName, durable, false, false, new JsonObject(queueArguments))
                .onComplete(v -> log.info("Queue '{}' for Dead Letter Exchange '{}' declared successfully.", dlxQueueName, dlxExchangeName),
                        queueError -> log.error("Failed to declare Dead Letter Queue '{}': {}", dlxQueueName, queueError.getMessage()));
    }

    @Getter
    private static final Map<String, Future<?>> queueConsumerFutures = new HashMap<>();

    private void processExchangeConsumerQueues(RabbitMQClient client, String targetExchangeName)
    {
        // Filter keys that map to the targetExchangeName
        List<String> matchingKeys = RabbitMQPreStartup.getQueueExchangeNames().entrySet()
                .stream()
                .filter(entry -> entry.getValue().equals(targetExchangeName)) // Match value to targetExchangeName
                .map(Map.Entry::getKey) // Extract the keys
                .toList();

        List<Future<Boolean>> out = new ArrayList<>();
        // Process each matching key
        for (String queueName : matchingKeys)
        {
            out.add(VertXPreStartup.getVertx().executeBlocking(() -> {
                log.debug("Processing queueName '{}' mapped to exchange '{}'", queueName, targetExchangeName);
                QueueDefinition queueDefinition = null;
                if (RabbitMQPreStartup.getQueueConsumerDefinitions().containsKey(queueName))
                {
                    queueDefinition = RabbitMQPreStartup.getQueueConsumerDefinitions().get(queueName);
                }
                if (queueDefinition != null)
                {
                    String routingKey = RabbitMQPreStartup.getQueueRoutingKeys().get(queueName);
                    String queueExchangeName = queueDefinition.exchange().isEmpty() || queueDefinition.exchange().equals("default") ? targetExchangeName : queueDefinition.exchange();
                    Class<? extends QueueConsumer> consumerClass = RabbitMQPreStartup.getQueueConsumerClass().get(queueName);
                    queueConsumerFutures.put(queueName, createQueue(client, routingKey, consumerClass, queueExchangeName));
                }
                return true;
            }, false));
        }
        Future.all(out).onComplete(ar -> {
            log.debug("All queue consumers for exchange '{}' have been created.", targetExchangeName);
        });
    }

    public Future<AMQP.Queue.DeclareOk> createQueue(RabbitMQClient rabbitMQClient, String routingKey, Class<? extends QueueConsumer> clazz, String exchangeName)
    {
        return createQueueWithRetries(rabbitMQClient, routingKey, clazz, exchangeName, 0, 2); // Start with attempt 0, max retries 2
    }

    private Future<AMQP.Queue.DeclareOk> createQueueWithRetries(RabbitMQClient rabbitMQClient, String routingKey, Class<? extends QueueConsumer> clazz, String exchangeName, int attempt, int maxRetries)
    {
        Future<AMQP.Queue.DeclareOk> out = null;
        QueueDefinition queueDefinition = clazz.getAnnotation(QueueDefinition.class);
        if (queueDefinition == null)
        {
            throw new RuntimeException("Queue definition not found for queue class: " + clazz.getName());
        }

        JsonObject queueConfig = new JsonObject();
        if (queueDefinition.options().ttl() != 0)
        {
            queueConfig.put("x-message-ttl", queueDefinition.options().ttl());
        }
        if (queueDefinition.options().singleConsumer())
        {
            queueConfig.put("x-single-active-consumer", true);
        }
        if (queueDefinition.options().priority() != 0)
        {
            queueConfig.put("x-max-priority", queueDefinition.options().priority());
        }

        log.debug("Attempting to declare queue '{}' (attempt {}/{})", queueDefinition.value(), attempt + 1, maxRetries);

        // Declare the queue
        return rabbitMQClient.queueDeclare(
                        queueDefinition.value(),
                        queueDefinition.options().durable(),
                        queueDefinition.options().consumerExclusive(),
                        queueDefinition.options().delete(),
                        queueConfig)
                .compose(result -> {
                    // If queueDeclare succeeds, attempt to bind the queue
                    log.info("Queue '{}' declared successfully.", queueDefinition.value());
                    Map<String, Object> arguments = new HashMap<>();
                    rabbitMQClient.queueBind(queueDefinition.value(), exchangeName, routingKey, arguments)
                            .onSuccess(bindResult -> log.debug("Queue '{}' bound to exchange '{}' with routing key '{}'.",
                                    queueDefinition.value(), exchangeName, routingKey))
                            .onFailure(bindError -> log.error("Failed to bind queue '{}' to exchange '{}': {}",
                                    queueDefinition.value(), exchangeName, bindError.getMessage()))
                            .onComplete(bindResult -> {
                                if (bindResult.succeeded())
                                {
                                    for (int i = 0; i < queueDefinition.options().consumerCount(); i++)
                                    {
                                        io.vertx.rabbitmq.QueueOptions qo = new QueueOptions();
                                        qo.setAutoAck(queueDefinition.options().autoAck());
                                        qo.setMaxInternalQueueSize(queueDefinition.options().maxInternalQueueSize());
                                        qo.setConsumerExclusive(queueDefinition.options().consumerExclusive());
                                        qo.setConsumerTag(buildConsumerTag(routingKey, i + 1));
                                        qo.setNoLocal(queueDefinition.options().noLocal());
                                        //qo.setKeepMostRecent(queueDefinition.options().keepMostRecent());
                                        //qo.setConsumerArguments();
                                        rabbitMQClient.basicConsumer(queueDefinition.value(), qo)
                                                .onSuccess(handler -> {
                                                    setupConsumer(rabbitMQClient, handler, routingKey, queueDefinition, clazz);
                                                });
                                    }
                                }
                            });

                    return Future.succeededFuture(result);
                })
                .recover(declareError -> {
                    log.warn("Failed to declare or bind queue '{}', attempt {}/{}: {}",
                            queueDefinition.value(), attempt + 1, maxRetries, declareError.getMessage());

                    // If max retry attempts reached, fail the Future
                    if (attempt >= maxRetries - 1)
                    {
                        log.error("Max retry attempts reached for queue '{}', failing permanently.", queueDefinition.value());
                        return Future.failedFuture(declareError); // Propagate error
                    }
                    // Delete the queue and retry
                    return rabbitMQClient.queueDelete(queueDefinition.value())
                            .onSuccess(deleteResult -> log.debug("Deleted queue '{}' successfully before retry.", queueDefinition.value()))
                            .compose(deleteResult -> {
                                log.info("Retrying queue declaration for '{}' (retry {}/{})", queueDefinition.value(), attempt + 2, maxRetries);
                                return createQueueWithRetries(rabbitMQClient, routingKey, clazz, exchangeName, attempt + 1, maxRetries); // Retry with incremented attempt
                            });
                });
    }


    private String buildConsumerTag(String routingKey, int index)
    {
        return routingKey + "_consumer_" + index;
    }

    private void setupConsumer(RabbitMQClient client, RabbitMQConsumer consumer, String routingKey, QueueDefinition queueDefinition, Class<? extends QueueConsumer> clazz)
    {
        log.debug("RabbitMQ consumer for queue '{}' setting up.", queueDefinition.value());
        //consumer.setQueueName(queueDefinition.value());
        consumer.fetch(queueDefinition.options().fetchCount());
        // Handle transacted vs. non-transacted consumers
        if (queueDefinition.options().transacted())
        {
            setupTransactedConsumer(client, consumer, queueDefinition, clazz);
        } else
        {
            setupStandardConsumer(client, consumer, queueDefinition, clazz);
        }

    }

    private void setupTransactedConsumer(RabbitMQClient client, RabbitMQConsumer consumer, QueueDefinition queueDefinition, Class<? extends QueueConsumer> clazz)
    {
        var tmc = IGuiceContext.get(Key.get(TransactedMessageConsumer.class, Names.named(queueDefinition.value())));
        tmc.setQueueDefinition(queueDefinition);
        tmc.setClazz(clazz);
        consumer.handler(message -> processMessageWithTransaction(client, tmc, message, queueDefinition, clazz));
    }

    private void processMessageWithTransaction(RabbitMQClient client, TransactedMessageConsumer tmc, RabbitMQMessage message, QueueDefinition queueDefinition, Class<? extends QueueConsumer> clazz)
    {
        var verticleOptional = VertXPreStartup.getAssociatedVerticle(clazz);
        Vertx vertx = null;
        if (verticleOptional.isEmpty())
        {
            vertx = VertXPreStartup.getVertx();
        } else
        {
            vertx = verticleOptional.get().getVertx();
        }

        vertx.executeBlocking(() -> {
            CallScoper scopedRunner = null;
            try
            {
                // Enter scoped transaction
                scopedRunner = IGuiceContext.get(CallScoper.class);
                scopedRunner.enter();

                var properties = IGuiceContext.get(CallScopeProperties.class);
                properties.setSource(CallScopeSource.RabbitMQ);

                log.trace("Running transacted message consumer for queue '{}'", queueDefinition.value());
                tmc.execute(clazz, message);

                if (!queueDefinition.options().autoAck())
                {
                    acknowledgeMessage(client, message, false);
                }
            } catch (Throwable t)
            {
                log.error("Error processing transacted message for queue '{}'", queueDefinition.value(), t);
                rejectMessage(client, message, false);
            } finally
            {
                // Exit transaction scope
                if (scopedRunner != null)
                {
                    scopedRunner.exit();
                }
            }
            return true;
        }, queueDefinition.options().singleConsumer());
    }

    private void setupStandardConsumer(RabbitMQClient client, RabbitMQConsumer consumer, QueueDefinition queueDefinition, Class<? extends QueueConsumer> clazz)
    {
        consumer.handler(message -> processMessage(client, message, queueDefinition, clazz));
    }

    private void processMessage(RabbitMQClient client, RabbitMQMessage message, QueueDefinition queueDefinition, Class<? extends QueueConsumer> clazz)
    {
        var verticleOptional = VertXPreStartup.getAssociatedVerticle(clazz);
        Vertx vertx = null;
        if (verticleOptional.isEmpty())
        {
            vertx = VertXPreStartup.getVertx();
        } else
        {
            vertx = verticleOptional.get().getVertx();
        }
        vertx.executeBlocking(() -> {
            CallScoper scopedRunner = null;
            try
            {
                // Enter a scoped context
                scopedRunner = IGuiceContext.get(CallScoper.class);
                scopedRunner.enter();

                var properties = IGuiceContext.get(CallScopeProperties.class);
                properties.setSource(CallScopeSource.RabbitMQ);

                log.trace("Processing standard message for queue '{}'", queueDefinition.value());
                var queueConsumer = IGuiceContext.get(Key.get(clazz, Names.named(queueDefinition.value())));
                queueConsumer.consume(message);

                if (!queueDefinition.options().autoAck())
                {
                    acknowledgeMessage(client, message, false);
                }
            } catch (Throwable t)
            {
                log.error("Error processing message for queue '{}'", queueDefinition.value(), t);
                rejectMessage(client, message, false);
            } finally
            {
                // Exit scoped context
                if (scopedRunner != null)
                {
                    scopedRunner.exit();
                }
            }
            return true;
        }, queueDefinition.options().singleConsumer());
    }

    private void acknowledgeMessage(RabbitMQClient client, RabbitMQMessage message, boolean multiple)
    {
        client.basicAck(message.envelope().getDeliveryTag(), multiple)
                .onComplete(result -> {
                    if (result.succeeded())
                    {
                        log.trace("Message successfully acknowledged.");
                    } else
                    {
                        log.error("Failed to acknowledge message.", result.cause());
                    }
                });
    }

    private void rejectMessage(RabbitMQClient client, RabbitMQMessage message, boolean requeue)
    {
        client.basicNack(message.envelope().getDeliveryTag(), false, requeue)
                .onComplete(result -> {
                    if (result.succeeded())
                    {
                        log.warn("Message successfully rejected (rejected: " + requeue + ").");
                    } else
                    {
                        log.error("Failed to reject message.", result.cause());
                    }
                });
    }

/*
    public static io.vertx.rabbitmq.QueueOptions toOptions(QueueOptions options)
    {
        io.vertx.rabbitmq.QueueOptions opt = new io.vertx.rabbitmq.QueueOptions();

        opt.setAutoAck(options.autoAck());
        opt.setConsumerExclusive(options.consumerExclusive());
        opt.setNoLocal(options.noLocal());
        opt.setKeepMostRecent(options.keepMostRecent());

        return opt;
    }
*/

    @Override
    public Integer sortOrder()
    {
        return Integer.MAX_VALUE - 200;
    }
}
