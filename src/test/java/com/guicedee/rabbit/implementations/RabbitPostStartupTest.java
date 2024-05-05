package com.guicedee.rabbit.implementations;

import com.guicedee.client.IGuiceContext;
import com.guicedee.rabbit.QueuePublisher;
import io.vertx.rabbitmq.RabbitMQClient;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;

class RabbitPostStartupTest
{
    @Inject
    @Named(value = "test-queue-consumer")
    private QueuePublisher queuePublisher;

    public static void main(String[] args)
    {
        new RabbitPostStartupTest().configure();
    }

    @Test
    void configure()
    {
        IGuiceContext.instance()
                     .getConfig()
                     .setClasspathScanning(true)
                     .setAnnotationScanning(true)
                     .setFieldScanning(true);

        RabbitMQClient rabbitMQClient = IGuiceContext.get(RabbitMQClient.class);
        RabbitPostStartupTest test = IGuiceContext.get(RabbitPostStartupTest.class);
        System.out.println("test");
        test.queuePublisher.publish("Test");
        System.out.println("sent");

    }
}