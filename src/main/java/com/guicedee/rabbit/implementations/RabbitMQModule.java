package com.guicedee.rabbit.implementations;

import com.google.common.base.Strings;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.name.Names;
import com.guicedee.client.IGuiceContext;
import com.guicedee.guicedinjection.interfaces.IGuiceModule;
import com.guicedee.rabbit.*;
import com.guicedee.rabbit.implementations.def.QueueOptionsDefault;
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
import java.util.*;
import java.util.logging.Level;

@Log
public class RabbitMQModule extends AbstractModule implements IGuiceModule<RabbitMQModule> {

    public static final Map<String, String> queueRoutingKeys = new HashMap<>();
    public static final Map<String, String> queueExchangeNames = new HashMap<>();
    public static final Map<String, RabbitMQClientProvider> queueConnections = new HashMap<>();
    public static final Map<String, RabbitMQConsumerProvider> queueConsumers = new HashMap<>();
    public static final Map<String, RabbitMQQueuePublisherProvider> queuePublishers = new HashMap<>();

    public static final Map<String, RabbitMQClientProvider> packageClients = new HashMap<>();
    public static final Map<String, RabbitMQClientProvider> exchangeClients = new HashMap<>();
    public static final Map<String, QueueExchange> exchangeDefinitions = new HashMap<>();
    public static final Map<String, String> packageExchanges = new HashMap<>();

    private List<ClassInfo> queuePublisherNames(ClassInfoList packageClasses) {
        Set<ClassInfo> boundKeys = new HashSet<>();
        for (ClassInfo classThatMay : packageClasses) {
            Class<?> aClass = classThatMay.loadClass(true);
            if (!getPublisherField(classThatMay).isEmpty()) {
                boundKeys.add(classThatMay);
            }
        }
        return new ArrayList<>(boundKeys);
    }

    private List<Field> getPublisherField(ClassInfo aClass) {
        List<Field> fields = new ArrayList<>();
        for (FieldInfo fieldInfo : aClass.getFieldInfo()) {
            if (!fieldInfo.isFinal() && !fieldInfo.isStatic()) {
                try {
                    Field declaredField = aClass.loadClass().getDeclaredField(fieldInfo.getName());
                    if (declaredField.isAnnotationPresent(Inject.class) &&
                            declaredField.isAnnotationPresent(Named.class) &&
                            QueuePublisher.class.isAssignableFrom(declaredField.getType())) {
                        Named annotation = declaredField.getAnnotation(Named.class);
                        fields.add(declaredField);
                        /*    String routingKey = queueExchangeName + "_" + annotation.value();
                            if (!routingKeysUsed.contains(routingKey)) {
                                bind(Key.get(QueuePublisher.class, Names.named(annotation.value())))
                                        .toProvider(new RabbitMQQueuePublisherProvider(annotation.value(), queueExchangeName, routingKey)).in(Singleton.class);
                            }*/
                    }
                } catch (Throwable e) {
                    //   e.printStackTrace();
                    //field not declared
                }
            }
        }
        return fields;
    }

    public void registerQueueConsumer(String queueName, String exchangeName, String routingKey, QueueDefinition queueDefinition) {

    }


    @Override
    protected void configure() {
        ScanResult scanResult = IGuiceContext.instance()
                .getScanResult();

        Set<Class<?>> completedConsumers = new HashSet<>();

        //break up per connection name
        ClassInfoList clientConnections = scanResult.getClassesWithAnnotation(RabbitConnectionOptions.class);
        boolean defaultBound = false;
        for (ClassInfo clientConnection : clientConnections) {
            RabbitConnectionOptions connectionOption = clientConnection.loadClass()
                    .getAnnotation(RabbitConnectionOptions.class);
            RabbitMQClientProvider clientProvider = new RabbitMQClientProvider(toOptions(connectionOption), connectionOption, clientConnection.getPackageName());
            requestInjection(clientProvider);
            if (clientConnections.size() == 1) {
                bind(RabbitMQClient.class).toProvider(clientProvider)
                        .asEagerSingleton();
            } else {
                if (!defaultBound) {
                    log.warning("Configuration default rabbit mq client to first found binding (should be first in order of loading) - " + connectionOption.value());
                    bind(RabbitMQClient.class).toProvider(clientProvider)
                            .asEagerSingleton();
                    defaultBound = true;
                }
            }

            packageClients.put(clientConnection.getPackageName(), clientProvider);

            bind(Key.get(RabbitMQClient.class, Names.named(connectionOption.value()))).toProvider(clientProvider);

            IGuiceContext.instance()
                    .loadPreDestroyServices()
                    .add(clientProvider);

            //find all queues in this package and map to this connection
            var ci = scanResult.getPackageInfo(clientConnection.getPackageName()).getClassInfoRecursive();
            var exchanges = ci.stream().filter(a -> a.hasAnnotation(QueueExchange.class)).toList();

            String exchangeName = "default";
            if (exchanges.isEmpty()) {
                throw new RuntimeException("No @QueueExchange Declared for Package - " + clientConnection.getPackageName());
            }


            for (ClassInfo exchange : exchanges) {
                var ex = exchange.loadClass().getAnnotation(QueueExchange.class);
                if (!Strings.isNullOrEmpty(ex.value()))
                    exchangeName = ex.value();

                exchangeDefinitions.put(exchangeName, ex);
                packageExchanges.put(clientConnection.getPackageName(), exchangeName);
                exchangeClients.put(exchangeName, clientProvider);

                var exchangePackageClasses = exchange.getPackageInfo().getClassInfoRecursive();
                var queueConsumers = exchangePackageClasses.stream().filter(a -> a.hasAnnotation(QueueDefinition.class)).toList();

                for (ClassInfo queueConsumerClass : queueConsumers) {
                    Class<QueueConsumer> aClass = (Class<QueueConsumer>) queueConsumerClass.loadClass();
                    if (completedConsumers.contains(aClass)) {
                        continue;
                    } else {
                        completedConsumers.add(aClass);
                    }
                    QueueDefinition queueDefinition = aClass.getAnnotation(QueueDefinition.class);
                    String queueName = queueDefinition.value();
                    queueExchangeNames.put(queueName, exchangeName);
                    String routingKey = exchangeName + "_" + queueDefinition.value();
                    RabbitMQModule.queueRoutingKeys.put(queueName, routingKey);
                    RabbitMQModule.queueConnections.put(queueName, clientProvider);
                    RabbitMQConsumerProvider provider = new RabbitMQConsumerProvider(clientProvider, queueDefinition, aClass, routingKey, exchangeName, clientProvider.getExchangeDeclared());
                    RabbitMQModule.queueConsumers.put(queueName, provider);
                }
            } //end of exchange binding
        }

        for (ClassInfo clientConnection : clientConnections) {
            var clientProvider = packageClients.get(clientConnection.getPackageName());
            RabbitConnectionOptions connectionOption = clientConnection.loadClass()
                    .getAnnotation(RabbitConnectionOptions.class);
            boolean confirmPublishes = connectionOption.confirmPublishes();

            var ci = scanResult.getPackageInfo(clientConnection.getPackageName()).getClassInfoRecursive();
            var exchanges = ci.stream().filter(a -> a.hasAnnotation(QueueExchange.class)).toList();

            String exchangeName = "default";
            if (exchanges.isEmpty()) {
                throw new RuntimeException("No @QueueExchange Declared for Package - " + clientConnection.getPackageName());
            }

            for (ClassInfo exchange : exchanges) {
                var ex = exchange.loadClass().getAnnotation(QueueExchange.class);
                if (!Strings.isNullOrEmpty(ex.value()))
                    exchangeName = ex.value();

                var exchangePackageClasses = exchange.getPackageInfo().getClassInfoRecursive();
                //          var queueConsumers = exchangePackageClasses.stream().filter(a -> a.hasAnnotation(QueueDefinition.class)).toList();

                //then publishers
                for (ClassInfo classWithPublisher : queuePublisherNames(exchangePackageClasses)) {
                    var fields = getPublisherField(classWithPublisher);
                    for (Field field : fields) {
                        var named = field.getAnnotation(Named.class);
                        var qd = field.getAnnotation(QueueDefinition.class);
                        String queueExchangeName = exchangeName;
                        String queueName = qd != null ? qd.value() : named.value();
                        String queueRoutingKey = queueName;
                        if (qd != null) {
                            exchangeName = qd.exchange();
                            queueExchangeName = exchangeName;
                            queueRoutingKey = exchangeName + "_" + qd.value();
                        } else {
                            queueRoutingKey = exchangeName + "_" + named.value();
                        }
                        if (queuePublishers.containsKey(queueRoutingKey)) {
                            continue;
                        }
                        var queueClientProvider = clientProvider;
                        if (queueConnections.containsKey(queueName)) {
                            var queueConsumerExchangeName = RabbitMQModule.queueExchangeNames.get(queueName);
                            queueExchangeName = queueConsumerExchangeName;
                            queueRoutingKey = RabbitMQModule.queueRoutingKeys.get(queueName);
                            queueClientProvider = queueConnections.get(queueName);
                        }

                        RabbitMQQueuePublisherProvider rabbitMQQueuePublisherProvider = null;
                        if (qd != null) {
                            rabbitMQQueuePublisherProvider = new RabbitMQQueuePublisherProvider(queueClientProvider, qd, queueExchangeName, queueRoutingKey, confirmPublishes);
                        } else {
                            rabbitMQQueuePublisherProvider = new RabbitMQQueuePublisherProvider(queueClientProvider, named.value(), queueExchangeName, queueRoutingKey, confirmPublishes);
                        }

                        RabbitMQModule.queuePublishers.put(queueName, rabbitMQQueuePublisherProvider);
                    }
                }
            } //end of exchange binding
        }


        Set<RabbitMQClient> declaredConnections = new HashSet<>();
        Set<Strings> declaredExchanges = new HashSet<>();
        Set<Strings> declaredQueues = new HashSet<>();

        //also per connection name
        bind(RabbitMQPublisher.class).toProvider(RabbitMQPublisherProvider.class).in(com.google.inject.Singleton.class);

        RabbitMQModule.queueConsumers.forEach((queueName, consumer) -> {
            IGuiceContext.instance()
                    .loadPreDestroyServices()
                    .add(consumer);
            Class<? extends QueueConsumer> aClass = consumer.getClazz();
            bind(Key.get(aClass)).in(Singleton.class);
            //   bind(Key.get(aClass,Names.named(consumer.getQueueDefinition().value()))).in(Singleton.class);
            bind(Key.get(TransactedMessageConsumer.class, Names.named(consumer.getQueueDefinition().value()))).to(TransactedMessageConsumer.class).in(Singleton.class);
            bind(Key.get(QueueConsumer.class, Names.named(consumer.getQueueDefinition().value()))).to(consumer.getClazz()).in(Singleton.class);

            /*if (consumer.getQueueDefinition().options()
                    .autobind()) {
                bind(Key.get(QueueConsumer.class, Names.named(consumer.getQueueDefinition().value()))).toProvider(consumer)
                        .asEagerSingleton();
            } else {
                bind(Key.get(QueueConsumer.class, Names.named(consumer.getQueueDefinition().value()))).toProvider(consumer);
            }*/
        });


        RabbitMQModule.queuePublishers.forEach((queueName, publisher) -> {
            bind(Key.get(QueuePublisher.class, Names.named(publisher.getQueueDefinition().value())))
                    .toProvider(publisher).in(Singleton.class);
        });
/*
        ClassInfoList queues = scanResult.getClassesWithAnnotation(QueueDefinition.class);
        ClassInfoList exchangeAnnotations = scanResult.getClassesWithAnnotation(QueueExchange.class);

        String queueExchangeName = "default";
        Set<String> routingKeysUsed = new HashSet<>();
        //bind the queues with their names to a RabbitMQConsumer
        for (ClassInfo queueClassInfo : queues) {
            Class<?> clazz = queueClassInfo.loadClass();
            Class<QueueConsumer> aClass = (Class<QueueConsumer>) clazz;
            QueueDefinition queueDefinition = aClass.getAnnotation(QueueDefinition.class);
            if (queueDefinition.exchange()
                    .equals("default") || exchangeAnnotations.size() == 1) {
                QueueExchange annotation = exchangeAnnotations.stream()
                        .findFirst()
                        .orElseThrow(() -> new RuntimeException("No @QueueExchange Declared for Connection - " + queueDefinition.value()))
                        .loadClass()
                        .getAnnotation(QueueExchange.class);
                queueExchangeName = annotation.value();
            }
            String routingKey = queueExchangeName + "_" + queueDefinition.value();
            String consumerCounter = "";
            //==== CONSUMERS =======
            String routingKeyCounted = routingKey + consumerCounter;
         //   RabbitMQConsumerProvider provider = new RabbitMQConsumerProvider(queueDefinition, aClass, routingKey, queueExchangeName);

        //    IGuiceContext.instance()
        //            .loadPreDestroyServices()
        //            .add(provider);

          //  bind(Key.get(aClass));
          //  bind(Key.get(aClass, Names.named(queueDefinition.value()))).to(aClass).in(Singleton.class);
       //     bind(Key.get(TransactedMessageConsumer.class, Names.named(queueDefinition.value()))).to(TransactedMessageConsumer.class).in(Singleton.class);

            if (queueDefinition.options()
                    .autobind()) {
        //        bind(Key.get(QueueConsumer.class, Names.named(queueDefinition.value()))).toProvider(provider)
       //                 .asEagerSingleton();
            } else {
          //      bind(Key.get(QueueConsumer.class, Names.named(queueDefinition.value()))).toProvider(provider);
            }
            if (!routingKeysUsed.contains(routingKey)) {
                routingKeysUsed.add(routingKey);
                //  RabbitMQQueuePublisherProvider rabbitMQQueuePublisherProvider = new RabbitMQQueuePublisherProvider(queueDefinition, queueExchangeName, routingKey);
                //  bind(Key.get(QueuePublisher.class, Names.named(queueDefinition.value())))
                //          .toProvider(rabbitMQQueuePublisherProvider).in(Singleton.class);
                //   IGuiceContext.instance()
                //          .loadPreDestroyServices()
                //          .add(rabbitMQQueuePublisherProvider);
            }

        }

        Set<String> boundKeys = new HashSet<>();
        for (ClassInfo classThatMay : scanResult.getAllClasses()) {
            Class<?> aClass = classThatMay.loadClass(true);
            for (FieldInfo fieldInfo : classThatMay.getFieldInfo()) {
                if (!fieldInfo.isFinal() && !fieldInfo.isStatic()) {
                    try {
                        Field declaredField = aClass.getDeclaredField(fieldInfo.getName());
                        if (declaredField.isAnnotationPresent(Inject.class) &&
                                declaredField.isAnnotationPresent(Named.class) &&
                                QueuePublisher.class.isAssignableFrom(declaredField.getType())) {
                            Named annotation = declaredField.getAnnotation(Named.class);
                            if (boundKeys.contains(annotation.value())) {
                                continue;
                            } else {
                                boundKeys.add(annotation.value());
                            }

                            String routingKey = queueExchangeName + "_" + annotation.value();
                            if (!routingKeysUsed.contains(routingKey)) {
                                //   bind(Key.get(QueuePublisher.class, Names.named(annotation.value())))
                                //              .toProvider(new RabbitMQQueuePublisherProvider(annotation.value(), queueExchangeName, routingKey)).in(Singleton.class);
                            }
                        }
                    } catch (Throwable e) {
                        //   e.printStackTrace();
                        //field not declared
                    }
                }
            }*/
        //}
    }

    private RabbitMQOptions toOptions(RabbitConnectionOptions options) {
        RabbitMQOptions opt = new RabbitMQOptions();
        if (options.reconnectAttempts() != 0) {
            opt.setReconnectAttempts(options.reconnectAttempts());
        }
        if (!Strings.isNullOrEmpty(options.host())) {
            opt.setHost(options.host());
        }
        if (options.port() != 0) {
            opt.setPort(options.port());
        }
        if (!Strings.isNullOrEmpty(options.user())) {
            opt.setUser(options.user());
        }
        if (!Strings.isNullOrEmpty(options.password())) {
            opt.setPassword(options.password());
        }
        if (!Strings.isNullOrEmpty(options.value())) {
            opt.setConnectionName(options.value());
        }

        opt.setAutomaticRecoveryEnabled(options.automaticRecoveryEnabled());
        if (!Strings.isNullOrEmpty(options.uri())) {
            opt.setUri(options.uri());
        }
        if (!Strings.isNullOrEmpty(options.virtualHost())) {
            opt.setVirtualHost(options.virtualHost());
        }
        if (options.connectionTimeout() != 0) {
            opt.setConnectionTimeout(options.connectionTimeout());
        }
        if (options.handshakeTimeout() != 0) {
            opt.setHandshakeTimeout(options.handshakeTimeout());
        }

        if (options.useNio()) {
            opt.setUseNio(options.useNio());
        }
        if (options.requestedChannelMax() != 0) {
            opt.setRequestedChannelMax(options.requestedChannelMax());
        }
        if (options.networkRecoveryInterval() != 0L) {
            opt.setNetworkRecoveryInterval(options.networkRecoveryInterval());
        }

        //opt.setAddresses(Arrays.asList(options.addresses()));
        return opt;
    }

}
