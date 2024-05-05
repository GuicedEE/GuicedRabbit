package com.guicedee.rabbit.implementations;

import com.google.inject.Provider;
import com.guicedee.rabbit.QueueDefinition;
import com.guicedee.rabbit.QueuePublisher;
import jakarta.inject.Singleton;

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

    @Override
    public QueuePublisher get()
    {
        QueuePublisher queuePublisher = new QueuePublisher(queueDefinition, exchangeName, routingKey);
        return queuePublisher;
    }

}
