package com.guicedee.rabbit.implementations;

import com.google.common.base.Strings;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.name.Names;
import com.guicedee.client.CallScoper;
import com.guicedee.client.Environment;
import com.guicedee.client.IGuiceContext;
import com.guicedee.guicedinjection.interfaces.IGuiceModule;
import com.guicedee.guicedservlets.websockets.options.CallScopeProperties;
import com.guicedee.guicedservlets.websockets.options.CallScopeSource;
import com.guicedee.rabbit.*;
import com.guicedee.rabbit.support.TransactedMessageConsumer;
import com.guicedee.vertx.spi.VertXPreStartup;
import com.rabbitmq.client.AMQP;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ClassInfoList;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.rabbitmq.QueueOptions;
import io.vertx.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQConsumer;
import io.vertx.rabbitmq.RabbitMQMessage;
import io.vertx.rabbitmq.RabbitMQOptions;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import lombok.extern.log4j.Log4j2;

import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

import static com.guicedee.rabbit.implementations.RabbitMQPreStartup.isFinal;
import static com.guicedee.rabbit.implementations.RabbitMQPreStartup.isStatic;

@Log4j2
public class RabbitMQModule extends AbstractModule implements IGuiceModule<RabbitMQModule>
{
    public static final Map<String, RabbitMQClient> packageClients = new HashMap<>();
    public static final Map<String, Future<?>> packageConnectionFutures = new HashMap<>();

    public static final Map<String, RabbitMQClient> exchangeClients = new HashMap<>();
    public static final Map<String, Future<?>> exchangeFutures = new HashMap<>();

    public static final Map<String, Future<?>> queueConsumerFutures = new HashMap<>();

    public static final Map<String, String> packageExchanges = new HashMap<>();

    private List<ClassInfo> queuePublisherNames(ClassInfoList packageClasses)
    {
        Set<ClassInfo> boundKeys = new HashSet<>();
        for (ClassInfo classThatMay : packageClasses)
        {
            Class<?> aClass = classThatMay.loadClass(true);
            if (!getPublisherField(classThatMay).isEmpty())
            {
                boundKeys.add(classThatMay);
            }
        }
        return new ArrayList<>(boundKeys);
    }

    private List<Field> getPublisherField(ClassInfo aClass)
    {
        // Load the class once to avoid multiple calls
        Class<?> clazz = aClass.loadClass();

        return Arrays.stream(clazz.getDeclaredFields())  // Stream the declared fields
                .filter(field -> !isFinal(field) && !isStatic(field)) // Filter out final and static fields
                .filter(field -> {
                    // Check annotation presence
                    boolean hasInject = field.isAnnotationPresent(Inject.class) || field.isAnnotationPresent(com.google.inject.Inject.class);
                    boolean hasNamed = field.isAnnotationPresent(Named.class) || field.isAnnotationPresent(com.google.inject.name.Named.class);
                    return hasInject && hasNamed;
                })
                .map(field -> {
                    try
                    {
                        // Attempt to re-fetch the field (if required)
                        return clazz.getDeclaredField(field.getName());
                    } catch (NoSuchFieldException | IllegalAccessError e)
                    {
                        // Log exception if necessary
                        log.debug("Field {} could not be loaded: {}", field.getName(), e.getMessage());
                        return null; // Null signals skipping this entry
                    }
                })
                .filter(Objects::nonNull) // Remove null entries due to exceptions
                .collect(Collectors.toList()); // Collect to the list
    }

    @Override
    protected void configure()
    {
        Set<String> completedPublishers = new HashSet<>();
        RabbitMQPreStartup.getPackageRabbitMqConnections().forEach((packageName, connections) -> {
            for (RabbitConnectionOptions connectionOption : connections)
            {
                var options = toOptions(connectionOption);
                boolean confirmPublishSet = connectionOption.confirmPublishes();
                if (!"5672".equalsIgnoreCase(Environment.getProperty("RABBIT_MQ_PORT", "5672")))
                {
                    options.setPort(Integer.parseInt(Environment.getProperty("RABBIT_MQ_PORT", "5672")));
                }
                var client = RabbitMQClient.create(VertXPreStartup.getVertx(), options);
                packageClients.put(packageName, client);
                packageConnectionFutures.put(packageName, client.start());
                bind(Key.get(RabbitMQClient.class, Names.named(connectionOption.value()))).toInstance(client);

                RabbitMQPreStartup.getPackageExchanges()
                        .keySet()
                        .stream()
                        .filter(a -> a.startsWith(packageName))
                        .forEach(exchangePackage -> {

                            var exchangeName = RabbitMQPreStartup.getPackageExchanges().get(exchangePackage);
                            if (!exchangeClients.containsKey(exchangeName))
                                exchangeClients.put(exchangeName, client);

                            var exchangeNameProperties = RabbitMQPreStartup.getExchangeDefinitions().get(exchangeName);
                            /*RabbitMQPreStartup.getQueueExchangeNames().forEach((queueName, exchangeName1) -> {
                                if(exchangeName1.equals(exchangeName))
                                {
                                    //bind a transactional consumer if needed
                                    bind(Key.get(TransactedMessageConsumer.class, Names.named(queueName))).toProvider(()->{
                                        return IGuiceContext.get(TransactedMessageConsumer.class);
                                    }).in(Singleton.class);
                                }
                            });*/
                            processExchangeRegistration(packageName, connectionOption, exchangeName, confirmPublishSet, client, exchangeNameProperties);
                        });

                //Consumers will start on the connection chain
                //todo this must move under, but keep the client and confirm option hmm
                RabbitMQPreStartup.getQueuePublisherDefinitions()
                        .forEach((queueName, queuePublisherDefinition) -> {
                            if (!completedPublishers.contains(queuePublisherDefinition.value()))
                            {
                                completedPublishers.add(queuePublisherDefinition.value());
                                bind(Key.get(QueuePublisher.class, Names.named(queuePublisherDefinition.value())))
                                        .toProvider(() -> {
                                            String exchangeName = RabbitMQPreStartup.getQueueExchangeNames().get(queueName);
                                            String routingKey = exchangeName + "_" + queuePublisherDefinition.value();
                                            QueuePublisher qp = new QueuePublisher(client, confirmPublishSet, queuePublisherDefinition,
                                                    exchangeName, routingKey);
                                            return qp;
                                        }).in(Singleton.class);
                            }
                        });
            }
        });

        RabbitMQPreStartup.getQueueConsumerDefinitions().forEach((queueName, queueConsumerDefinition) -> {
            Class clazz = RabbitMQPreStartup.getQueueConsumerClass().get(queueName);
            var clazzy = RabbitMQPreStartup.getQueueConsumerClass().get(queueName);
            bind(clazz).in(Singleton.class);
            bind(Key.get(clazz,Names.named(queueConsumerDefinition.value())))
                    .to(clazz);
            bind(Key.get(QueueConsumer.class,Names.named(queueConsumerDefinition.value())))
                    .to(clazz);
            bind(Key.get(TransactedMessageConsumer.class, Names.named(queueName))).toProvider(()->{
                return IGuiceContext.get(TransactedMessageConsumer.class);
            }).in(Singleton.class);
                    //.toProvider(()-> IGuiceContext.get(Key.get(clazzy,Names.named(queueConsumerDefinition.value())))).in(Singleton.class);
        });

        //also per connection name
        /*bind(RabbitMQPublisher.class).toProvider(RabbitMQPublisherProvider.class).in(com.google.inject.Singleton.class);

        RabbitMQModule.queueConsumers.forEach((queueName, consumer) -> {
            IGuiceContext.instance()
                    .loadPreDestroyServices()
                    .add(consumer);
            Class<? extends QueueConsumer> aClass = consumer.getClazz();
            bind(Key.get(aClass)).in(Singleton.class);
            bind(Key.get(TransactedMessageConsumer.class, Names.named(consumer.getQueueDefinition().value()))).to(TransactedMessageConsumer.class).in(Singleton.class);
            bind(Key.get(QueueConsumer.class, Names.named(consumer.getQueueDefinition().value()))).to(consumer.getClazz()).in(Singleton.class);
        });*/

      /*  RabbitMQModule.queuePublishers.forEach((queueName, publisher) -> {
            bind(Key.get(QueuePublisher.class, Names.named(publisher.getQueueDefinition().value())))
                    .toProvider(publisher).in(Singleton.class);
        });*/
    }

    private void processExchangeRegistration(String packageName, RabbitConnectionOptions connectionOption, String exchangeName, boolean confirmPublishSet, RabbitMQClient client, QueueExchange exchangeNameProperties)
    {
        packageConnectionFutures.get(packageName)
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
                        log.info("Creating Dead Letter Exchange '{}'", dlxName);
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
        queueArguments.put("x-message-ttl", 60000); // Optional: TTL example (1 minute)
        queueArguments.put("x-max-length", 10000); // Optional: Limit max queue size (10,000 messages)

        String dlxQueueName = queueName + ".DLX"; // Name of the queue bound to the DLX
        return client.queueDeclare(dlxQueueName, durable, false, false, new JsonObject(queueArguments))
                .onComplete(v -> log.info("Queue '{}' for Dead Letter Exchange '{}' declared successfully.", dlxQueueName, dlxExchangeName),
                        queueError -> log.error("Failed to declare Dead Letter Queue '{}': {}", dlxQueueName, queueError.getMessage()));
    }

    private void processExchangeConsumerQueues(RabbitMQClient client, String targetExchangeName)
    {
        // Filter keys that map to the targetExchangeName
        List<String> matchingKeys = RabbitMQPreStartup.getQueueExchangeNames().entrySet()
                .stream()
                .filter(entry -> entry.getValue().equals(targetExchangeName)) // Match value to targetExchangeName
                .map(Map.Entry::getKey) // Extract the keys
                .toList();
        // Process each matching key
        for (String queueName : matchingKeys)
        {
            log.info("Processing queueName '{}' mapped to exchange '{}'", queueName, targetExchangeName);
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
        }
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

        log.info("Attempting to declare queue '{}' (attempt {}/{})", queueDefinition.value(), attempt + 1, maxRetries);

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
                            .onSuccess(bindResult -> log.info("Queue '{}' bound to exchange '{}' with routing key '{}'.",
                                    queueDefinition.value(), exchangeName, routingKey))
                            .onFailure(bindError -> log.error("Failed to bind queue '{}' to exchange '{}': {}",
                                    queueDefinition.value(), exchangeName, bindError.getMessage()))
                            .onComplete(bindResult -> {
                                if (bindResult.succeeded())
                                {
                                    for (int i = 0; i < queueDefinition.options().consumerCount(); i++)
                                    {
                                        QueueOptions qo = new QueueOptions();
                                        qo.setAutoAck(queueDefinition.options().autoAck());
                                        qo.setMaxInternalQueueSize(queueDefinition.options().maxInternalQueueSize());
                                        qo.setConsumerExclusive(queueDefinition.options().consumerExclusive());
                                        qo.setConsumerTag(buildConsumerTag(routingKey, i + 1));
                                        qo.setNoLocal(queueDefinition.options().noLocal());
                                        qo.setKeepMostRecent(queueDefinition.options().keepMostRecent());
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
                            .onSuccess(deleteResult -> log.info("Deleted queue '{}' successfully before retry.", queueDefinition.value()))
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
        log.info("RabbitMQ consumer for queue '{}' created successfully.", queueDefinition.value());
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
        IGuiceContext.instance().getLoadingFinished().onSuccess(handler -> {
            var tmc = IGuiceContext.get(Key.get(TransactedMessageConsumer.class, Names.named(queueDefinition.value())));
            tmc.setQueueDefinition(queueDefinition);
            tmc.setClazz(clazz);
            consumer.handler(message -> processMessageWithTransaction(client, tmc, message, queueDefinition, clazz));
        });
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


    private RabbitMQOptions toOptions(RabbitConnectionOptions options)
    {
        RabbitMQOptions opt = new RabbitMQOptions();
        if (options.reconnectAttempts() != 0)
        {
            opt.setReconnectAttempts(options.reconnectAttempts());
        }
        if (!Strings.isNullOrEmpty(options.host()))
        {
            opt.setHost(options.host());
        }
        if (options.port() != 0)
        {
            opt.setPort(options.port());
        }
        if (!Strings.isNullOrEmpty(options.user()))
        {
            opt.setUser(options.user());
        }
        if (!Strings.isNullOrEmpty(options.password()))
        {
            opt.setPassword(options.password());
        }
        if (!Strings.isNullOrEmpty(options.value()))
        {
            opt.setConnectionName(options.value());
        }

        opt.setAutomaticRecoveryEnabled(options.automaticRecoveryEnabled());
        if (!Strings.isNullOrEmpty(options.uri()))
        {
            opt.setUri(options.uri());
        }
        if (!Strings.isNullOrEmpty(options.virtualHost()))
        {
            opt.setVirtualHost(options.virtualHost());
        }
        if (options.connectionTimeout() != 0)
        {
            opt.setConnectionTimeout(options.connectionTimeout());
        }
        if (options.handshakeTimeout() != 0)
        {
            opt.setHandshakeTimeout(options.handshakeTimeout());
        }

        if (options.useNio())
        {
            opt.setUseNio(options.useNio());
        }
        if (options.requestedChannelMax() != 0)
        {
            opt.setRequestedChannelMax(options.requestedChannelMax());
        }
        if (options.networkRecoveryInterval() != 0L)
        {
            opt.setNetworkRecoveryInterval(options.networkRecoveryInterval());
        }

        //opt.setAddresses(Arrays.asList(options.addresses()));
        return opt;
    }

}
