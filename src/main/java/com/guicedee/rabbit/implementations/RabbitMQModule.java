package com.guicedee.rabbit.implementations;

import com.google.common.base.Strings;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.name.Names;
import com.guicedee.client.IGuiceContext;
import com.guicedee.guicedinjection.interfaces.IGuiceModule;
import com.guicedee.rabbit.*;
import com.guicedee.rabbit.implementations.def.RabbitMQClientProvider;
import com.guicedee.rabbit.support.TransactedMessageConsumer;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.FieldInfo;
import io.github.classgraph.ScanResult;
import io.vertx.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQOptions;
import io.vertx.rabbitmq.RabbitMQPublisher;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import lombok.extern.java.Log;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

@QueueExchange
@Log
public class RabbitMQModule extends AbstractModule implements IGuiceModule<RabbitMQModule>
{
    @Override
    protected void configure()
    {
        ScanResult scanResult = IGuiceContext.instance()
                                             .getScanResult();

        //break up per connection name
        ClassInfoList clientConnections = scanResult.getClassesWithAnnotation(RabbitConnectionOptions.class);
        boolean defaultBound= false;
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
            }else {
                if (!defaultBound)
                {
                    log.warning("Configuration default rabbit mq client to first found binding (should be first in order of loading) - " + connectionOption.value());
                    bind(RabbitMQClient.class).toProvider(clientProvider)
                                              .asEagerSingleton();
                    defaultBound = true;
                }
            }
            bind(Key.get(RabbitMQClient.class, Names.named(connectionOption.value()))).toProvider(clientProvider);
            IGuiceContext.instance()
                         .loadPreDestroyServices()
                         .add(clientProvider);
        }
        //also per connection name
        bind(RabbitMQPublisher.class).toProvider(RabbitMQPublisherProvider.class).in(com.google.inject.Singleton.class);


        ClassInfoList queues = scanResult.getClassesWithAnnotation(QueueDefinition.class);
        ClassInfoList exchangeAnnotations = scanResult.getClassesWithAnnotation(QueueExchange.class);

        String queueExchangeName = "default";
        Set<String> routingKeysUsed = new HashSet<>();
        //bind the queues with their names to a RabbitMQConsumer
        for (ClassInfo queueClassInfo : queues)
        {
            Class<?> clazz = queueClassInfo.loadClass();
            Class<QueueConsumer> aClass = (Class<QueueConsumer>) clazz;
            QueueDefinition queueDefinition = aClass.getAnnotation(QueueDefinition.class);
            if (queueDefinition.exchange()
                               .equals("default") || exchangeAnnotations.size() == 1)
            {
                QueueExchange annotation = exchangeAnnotations.stream()
                                                              .findFirst()
                                                              .orElseThrow(() -> new RuntimeException("No @QueueExchange Declared for Connection - " + queueDefinition.value()))
                                                              .loadClass()
                                                              .getAnnotation(QueueExchange.class);
                queueExchangeName = annotation.value();
            }
            String routingKey = queueExchangeName + "_" + queueDefinition.value();

            //queueDefault.setOptions(options);
            RabbitMQConsumerProvider provider = new RabbitMQConsumerProvider(queueDefinition, aClass, routingKey, queueExchangeName);
            IGuiceContext.instance()
                         .loadPreDestroyServices()
                         .add(provider);
            bind(Key.get(aClass));
            bind(Key.get(aClass,Names.named(queueDefinition.value()))).to(aClass).in(Singleton.class);
            bind(Key.get(TransactedMessageConsumer.class,Names.named(queueDefinition.value()))).to(TransactedMessageConsumer.class).in(Singleton.class);

            if (queueDefinition.options()
                               .autobind())
            {
                bind(Key.get(QueueConsumer.class, Names.named(queueDefinition.value()))).toProvider(provider)
                                                                                        .asEagerSingleton();
            }
            else
            {
                bind(Key.get(QueueConsumer.class, Names.named(queueDefinition.value()))).toProvider(provider);
            }
            if (!routingKeysUsed.contains(routingKey))
            {
                routingKeysUsed.add(routingKey);
                RabbitMQQueuePublisherProvider rabbitMQQueuePublisherProvider = new RabbitMQQueuePublisherProvider(queueDefinition, queueExchangeName, routingKey);
                bind(Key.get(QueuePublisher.class, Names.named(queueDefinition.value())))
                        .toProvider(rabbitMQQueuePublisherProvider).in(Singleton.class);
                IGuiceContext.instance()
                             .loadPreDestroyServices()
                             .add(rabbitMQQueuePublisherProvider);
            }
        }

        Set<String> boundKeys = new HashSet<>();
        for (ClassInfo classThatMay : scanResult.getAllClasses())
        {
            Class<?> aClass = classThatMay.loadClass(true);
            for (FieldInfo fieldInfo : classThatMay.getFieldInfo())
            {
                if (!fieldInfo.isFinal() && !fieldInfo.isStatic())
                {
                    try
                    {
                        Field declaredField = aClass.getDeclaredField(fieldInfo.getName());
                        if (declaredField.isAnnotationPresent(Inject.class) &&
                                declaredField.isAnnotationPresent(Named.class) &&
                                QueuePublisher.class.isAssignableFrom(declaredField.getType()))
                        {
                            Named annotation = declaredField.getAnnotation(Named.class);
                            if (boundKeys.contains(annotation.value()))
                            {
                                continue;
                            }
                            else
                            {
                                boundKeys.add(annotation.value());
                            }

                            String routingKey = queueExchangeName + "_" + annotation.value();
                            if (!routingKeysUsed.contains(routingKey))
                            {
                                bind(Key.get(QueuePublisher.class, Names.named(annotation.value())))
                                        .toProvider(new RabbitMQQueuePublisherProvider(annotation.value(), queueExchangeName, routingKey)).in(Singleton.class);
                            }
                        }
                    }
                    catch (Throwable e)
                    {
                        //   e.printStackTrace();
                        //field not declared
                    }
                }
            }
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
