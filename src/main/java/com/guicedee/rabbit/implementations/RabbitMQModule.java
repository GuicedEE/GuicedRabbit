package com.guicedee.rabbit.implementations;

import com.google.common.base.Strings;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import com.guicedee.client.Environment;
import com.guicedee.client.IGuiceContext;
import com.guicedee.guicedinjection.interfaces.IGuiceModule;
import com.guicedee.rabbit.QueueConsumer;
import com.guicedee.rabbit.QueuePublisher;
import com.guicedee.rabbit.RabbitConnectionOptions;
import com.guicedee.rabbit.support.TransactedMessageConsumer;
import com.guicedee.vertx.spi.VertXPreStartup;
import io.vertx.core.Future;
import io.vertx.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQOptions;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

import java.util.*;

;

@Log4j2
public class RabbitMQModule extends AbstractModule implements IGuiceModule<RabbitMQModule>
{
    @Getter
    private static final Map<String, RabbitMQClient> packageClients = new HashMap<>();
    @Getter
    private static final Map<String, Future<?>> packageConnectionFutures = new HashMap<>();

    @Getter
    private static final Map<String, RabbitMQClient> exchangeClients = new HashMap<>();

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
                        });
            }
        });

        RabbitMQPreStartup.getQueueConsumerDefinitions().forEach((queueName, queueConsumerDefinition) -> {
            Class clazz = RabbitMQPreStartup.getQueueConsumerClass().get(queueName);
            var clazzy = RabbitMQPreStartup.getQueueConsumerClass().get(queueName);
            bind(clazz).in(Singleton.class);
            bind(Key.get(clazz, Names.named(queueConsumerDefinition.value())))
                    .to(clazz);
            bind(Key.get(QueueConsumer.class, Names.named(queueConsumerDefinition.value())))
                    .to(clazz);
            bind(Key.get(TransactedMessageConsumer.class, Names.named(queueName))).toProvider(() -> {
                return IGuiceContext.get(TransactedMessageConsumer.class);
            }).in(Singleton.class);
            //.toProvider(()-> IGuiceContext.get(Key.get(clazzy,Names.named(queueConsumerDefinition.value())))).in(Singleton.class);
        });

        RabbitMQPreStartup.getPackageRabbitMqConnections().forEach((packageName, connections) -> {
                    for (RabbitConnectionOptions connectionOption : connections)
                    {
                        var client = packageClients.get(packageName);
                        boolean confirmPublishSet = connectionOption.confirmPublishes();

                        RabbitMQPreStartup.getQueuePublisherDefinitions(packageName)
                                .forEach((queueName, queuePublisherDefinition) -> {
                                    if (!completedPublishers.contains(queuePublisherDefinition.value()))
                                    {
                                        completedPublishers.add(queuePublisherDefinition.value());
                                        bind(Key.get(QueuePublisher.class, Names.named(queuePublisherDefinition.value())))
                                                .toProvider(() -> {
                                                    String exchangeName = RabbitMQPreStartup.getQueueExchangeNames().get(queueName);
                                                    if (Strings.isNullOrEmpty(exchangeName))
                                                    {
                                                        log.error("Queue {} has no exchange defined. Cannot publish to it.", queueName);
                                                        return null;
                                                    }
                                                    String routingKey = exchangeName + "_" + queuePublisherDefinition.value();
                                                    QueuePublisher qp = new QueuePublisher(client, confirmPublishSet, queuePublisherDefinition,
                                                            exchangeName, routingKey);
                                                    return qp;
                                                }).in(Singleton.class);
                                    }
                                });
                    }
                }
        );
        //then the remaining to the first connection found
        for (Map.Entry<String, List<RabbitConnectionOptions>> entry : RabbitMQPreStartup.getPackageRabbitMqConnections().entrySet())
        {
            String packageName = entry.getKey();
            List<RabbitConnectionOptions> connections = entry.getValue();
            RabbitConnectionOptions connectionOption = connections.get(0);
            var client = packageClients.get(packageName);
            boolean confirmPublishSet = connectionOption.confirmPublishes();

            RabbitMQPreStartup.getQueuePublisherDefinitions()
                    .forEach((queueName, queuePublisherDefinition) -> {
                        if (!completedPublishers.contains(queuePublisherDefinition.value()))
                        {
                            completedPublishers.add(queuePublisherDefinition.value());
                            bind(Key.get(QueuePublisher.class, Names.named(queuePublisherDefinition.value())))
                                    .toProvider(() -> {
                                        String exchangeName = RabbitMQPreStartup.getQueueExchangeNames().get(queueName);
                                        if (Strings.isNullOrEmpty(exchangeName))
                                        {
                                            log.error("Queue {} has no exchange defined. Cannot publish to it.", queueName);
                                            return null;
                                        }
                                        String routingKey = exchangeName + "_" + queuePublisherDefinition.value();
                                        QueuePublisher qp = new QueuePublisher(client, confirmPublishSet, queuePublisherDefinition,
                                                exchangeName, routingKey);
                                        return qp;
                                    }).in(Singleton.class);
                        }
                    });
            break;
        }
    }


    public static RabbitMQOptions toOptions(RabbitConnectionOptions options)
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
