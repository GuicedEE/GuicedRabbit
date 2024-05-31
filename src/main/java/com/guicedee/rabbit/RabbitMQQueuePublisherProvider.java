package com.guicedee.rabbit;

import com.google.inject.Provider;
import com.guicedee.rabbit.implementations.def.QueueOptionsDefault;
import jakarta.inject.Singleton;

import java.lang.annotation.Annotation;

@Singleton
public class RabbitMQQueuePublisherProvider implements Provider<QueuePublisher>
{
    private final QueueDefinition queueDefinition;
    private final String exchangeName;
    private final String routingKey;

    public RabbitMQQueuePublisherProvider(QueueDefinition queueDefinition, String exchangeName, String routingKey)
    {
        this.queueDefinition = queueDefinition;
        this.exchangeName = exchangeName;
        this.routingKey = routingKey;
    }


    public RabbitMQQueuePublisherProvider(String queueDefinition, String exchangeName, String routingKey)
    {
        this.queueDefinition = new QueueDefinition(){
            @Override
            public Class<? extends Annotation> annotationType()
            {
                return QueueDefinition.class;
            }

            @Override
            public String value()
            {
                return queueDefinition;
            }

            @Override
            public QueueOptions options()
            {
                return new QueueOptionsDefault();
            }

            @Override
            public String exchange()
            {
                return exchangeName;
            }
        };
        this.exchangeName = exchangeName;
        this.routingKey = routingKey;
    }

    @Override
    public QueuePublisher get()
    {
        QueuePublisher queuePublisher = new QueuePublisher(queueDefinition, exchangeName, routingKey);
        return queuePublisher;
    }

}
