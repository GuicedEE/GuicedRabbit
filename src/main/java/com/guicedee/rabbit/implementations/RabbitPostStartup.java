package com.guicedee.rabbit.implementations;

import com.google.inject.Key;
import com.google.inject.name.Names;
import com.guicedee.client.IGuiceContext;
import com.guicedee.guicedinjection.interfaces.IGuicePostStartup;
import com.guicedee.rabbit.QueueConsumer;
import com.guicedee.rabbit.QueueDefinition;
import com.guicedee.rabbit.QueueOptions;
import com.guicedee.rabbit.QueuePublisher;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import lombok.extern.java.Log;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;


@Log
public class RabbitPostStartup implements IGuicePostStartup<RabbitPostStartup> {

    @Override
    public List<CompletableFuture<Boolean>> postLoad() {
        ScanResult scanResult = IGuiceContext.instance()
                .getScanResult();

        List<CompletableFuture<Boolean>> futures = new ArrayList<>();
        CompletableFuture<Boolean> objectCompletableFuture = new CompletableFuture<>().newIncompleteFuture();
        futures.add(objectCompletableFuture);

        RabbitMQModule.packageClients.forEach((name, connection) -> {
            connection.get().start();
        });

/*        RabbitMQModule.queueConsumers.forEach((name, queueConsumer) -> {
            Class<QueueConsumer> aClass = (Class<QueueConsumer>) queueConsumer.getClazz();
            IGuiceContext.get(aClass);
        });*/

        ClassInfoList queues = scanResult.getClassesWithAnnotation(QueueDefinition.class);
        for (ClassInfo queueClassInfo : queues) {
            Class<?> clazz = queueClassInfo.loadClass();
            Class<QueueConsumer> aClass = (Class<QueueConsumer>) clazz;
            objectCompletableFuture.complete(true);
            QueueDefinition queueDefinition = aClass.getAnnotation(QueueDefinition.class);
            log.config("Starting Queue Consumer - " + queueDefinition.value());
            Key<QueuePublisher> queuePublisherKey = Key.get(QueuePublisher.class, Names.named(queueDefinition.value()));
        }
        futures.get(0).complete(true);
        return futures;
    }

    public static io.vertx.rabbitmq.QueueOptions toOptions(QueueOptions options) {
        io.vertx.rabbitmq.QueueOptions opt = new io.vertx.rabbitmq.QueueOptions();

        opt.setAutoAck(options.autoAck());
        opt.setConsumerExclusive(options.consumerExclusive());
        opt.setNoLocal(options.noLocal());
        opt.setKeepMostRecent(options.keepMostRecent());

        return opt;
    }

    @Override
    public Integer sortOrder() {
        return Integer.MIN_VALUE + 600;
    }
}
