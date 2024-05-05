package com.guicedee.rabbit.implementations;

import com.google.inject.Key;
import com.google.inject.name.Names;
import com.guicedee.client.IGuiceContext;
import com.guicedee.guicedinjection.interfaces.IGuicePostStartup;
import com.guicedee.rabbit.QueueConsumer;
import com.guicedee.rabbit.QueueDefinition;
import com.guicedee.rabbit.QueueOptions;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import lombok.extern.java.Log;

@Log
public class RabbitPostStartup implements IGuicePostStartup<RabbitPostStartup>
{
    @Override
    public void postLoad()
    {
        ScanResult scanResult = IGuiceContext.instance()
                                             .getScanResult();
        ClassInfoList queues = scanResult.getClassesWithAnnotation(QueueDefinition.class);
        for (ClassInfo queueClassInfo : queues)
        {
            Class<?> clazz = queueClassInfo.loadClass();
            Class<QueueConsumer> aClass = (Class<QueueConsumer>) clazz;
            QueueDefinition queueDefinition = aClass.getAnnotation(QueueDefinition.class);
            log.config("Starting Queue Consumer - " + queueDefinition.value());
            QueueConsumer queueConsumer = IGuiceContext.get(Key.get(QueueConsumer.class, Names.named(queueDefinition.value())));
            log.config("Started Queue Consumer - " + queueConsumer);
        }


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
}
