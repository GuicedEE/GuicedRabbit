package com.guicedee.rabbit.implementations;

import com.guicedee.client.IGuiceContext;
import com.guicedee.rabbit.QueuePublisher;
import com.google.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

class RabbitPostStartupTest
{
    @Inject
    @Named(value = "test-queue-consumer")
    private QueuePublisher queuePublisher;

    @Inject
    @Named(value = "single-consumer-test")
    private QueuePublisher singleConsumer;


    @Inject
    @Named(value = "not-included-queue")
    private QueuePublisher blankConsumer;


    public static void main(String[] args)
    {
        new RabbitPostStartupTest().configure();
    }

    @Test
    void configure()
    {
        try (var rabbit = new RabbitMQContainer(DockerImageName.parse("rabbitmq:3.7.25-management-alpine")))
        {
            rabbit.setExposedPorts(List.of(5672));
            rabbit.setHostAccessible(true);
            rabbit.setPortBindings(List.of("5672"));
            rabbit.start();

            IGuiceContext.instance()
                         .getConfig()
                         .setClasspathScanning(true)
                         .setAnnotationScanning(true)
                         .setFieldScanning(true);

           /* RabbitMQClient rabbitMQClient = IGuiceContext.get(RabbitMQClient.class);
            RabbitPostStartupTest test = IGuiceContext.get(RabbitPostStartupTest.class);
            System.out.println("test");
            test.queuePublisher.publish("Test");
            System.out.println("sent");
            test.singleConsumer.publish("Tester");
            System.out.println("sent 2");*/
        }
    }
}