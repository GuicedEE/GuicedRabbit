package com.guicedee.rabbit.implementations;

import com.google.common.base.Strings;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.name.Names;
import com.guicedee.client.IGuiceContext;
import com.guicedee.guicedinjection.interfaces.IGuiceModule;
import com.guicedee.rabbit.*;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import io.vertx.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQOptions;
import io.vertx.rabbitmq.RabbitMQPublisher;
import jakarta.inject.Singleton;

@QueueExchange
public class RabbitMQModule extends AbstractModule implements IGuiceModule<RabbitMQModule>
{
    @Override
    protected void configure()
    {
        ScanResult scanResult = IGuiceContext.instance()
                                             .getScanResult();

        //break up per connection name
        ClassInfoList clientConnections = scanResult.getClassesWithAnnotation(RabbitConnectionOptions.class);
        for (ClassInfo clientConnection : clientConnections)
        {
            RabbitConnectionOptions connectionOption = clientConnection.loadClass()
                                                                       .getAnnotation(RabbitConnectionOptions.class);
            RabbitMQClientProvider clientProvider = new RabbitMQClientProvider(toOptions(connectionOption));
            requestInjection(clientProvider);
            if (clientConnections.size() == 1)
            {
                bind(RabbitMQClient.class).toProvider(clientProvider)
                                          .asEagerSingleton();
            }
            bind(Key.get(RabbitMQClient.class, Names.named(connectionOption.value()))).toProvider(clientProvider);
        }
        //also per connection name
        bind(RabbitMQPublisher.class).toProvider(RabbitMQPublisherProvider.class)
                                     .in(Singleton.class);

        ClassInfoList queues = scanResult.getClassesWithAnnotation(QueueDefinition.class);
        ClassInfoList exchangeAnnotations = scanResult.getClassesWithAnnotation(QueueExchange.class);

        //bind the queues with their names to a RabbitMQConsumer
        for (ClassInfo queueClassInfo : queues)
        {
            Class<?> clazz = queueClassInfo.loadClass();
            Class<QueueConsumer> aClass = (Class<QueueConsumer>) clazz;
            QueueDefinition queueDefinition = aClass.getAnnotation(QueueDefinition.class);
            String queueExchangeName = "default";
            if (queueDefinition.exchange()
                               .equals("default") || exchangeAnnotations.size() == 1)
            {
                QueueExchange annotation = exchangeAnnotations.stream()
                                                              .findFirst()
                                                              .orElseThrow()
                                                              .loadClass()
                                                              .getAnnotation(QueueExchange.class);
                queueExchangeName = annotation.value();
            }
            //queueDefault.setOptions(options);
            RabbitMQConsumerProvider provider = new RabbitMQConsumerProvider(queueDefinition, aClass);
            if (queueDefinition.options()
                               .singleConsumer())
            {
                bind(Key.get(aClass)).toProvider(provider);
                bind(Key.get(QueueConsumer.class, Names.named(queueDefinition.value()))).to(Key.get(aClass));
            }
            else
            {
                bind(Key.get(aClass)).toProvider(provider);
                bind(Key.get(QueueConsumer.class, Names.named(queueDefinition.value()))).to(Key.get(aClass));
            }

            String routingKey = queueExchangeName + "_" + queueDefinition.value();
            bind(Key.get(QueuePublisher.class, Names.named(queueDefinition.value())))
                    .toProvider(new RabbitMQQueuePublisherProvider(queueDefinition, queueExchangeName, routingKey));
        }
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
        //opt.setAddresses(Arrays.asList(options.addresses()));
        return opt;
    }

}
