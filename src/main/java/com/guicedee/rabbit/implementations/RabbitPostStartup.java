package com.guicedee.rabbit.implementations;

import com.google.inject.Inject;
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
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import lombok.extern.java.Log;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;


@Log
public class RabbitPostStartup implements IGuicePostStartup<RabbitPostStartup>
{

    @Inject
    private Vertx vertx;

    @Override
    public List<Future<Boolean>> postLoad()
    {
        Promise<Boolean> promise = Promise.promise();

        List<Future<?>> allConsumers = new ArrayList<>();
        RabbitMQPreStartup.getQueueConsumerDefinitions().forEach((name, queueDefinition) -> {
            allConsumers.add(vertx.executeBlocking(() -> {
                log.config("Starting Queue Consumer - " + queueDefinition.value());
                Key<QueuePublisher> queuePublisherKey = Key.get(QueuePublisher.class, Names.named(queueDefinition.value()));
                IGuiceContext.get(queuePublisherKey);
                return true;
            }, false));
        });
        Future.all(allConsumers).onComplete(ar -> {
            if (ar.succeeded())
                promise.complete(ar.succeeded());
            else
            {
                promise.fail(ar.cause());
            }
        });

        return List.of(promise.future());
    }

    public static io.vertx.rabbitmq.QueueOptions toOptions(QueueOptions options)
    {
        io.vertx.rabbitmq.QueueOptions opt = new io.vertx.rabbitmq.QueueOptions();

        opt.setAutoAck(options.autoAck());
        opt.setConsumerExclusive(options.consumerExclusive());
        opt.setNoLocal(options.noLocal());
        opt.setKeepMostRecent(options.keepMostRecent());

        return opt;
    }

    @Override
    public Integer sortOrder()
    {
        return Integer.MIN_VALUE + 600;
    }
}
