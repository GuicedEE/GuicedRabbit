package com.guicedee.rabbit;

import com.google.inject.Provider;
import com.guicedee.guicedinjection.interfaces.IGuicePreDestroy;
import com.guicedee.rabbit.implementations.def.QueueOptionsDefault;
import com.guicedee.rabbit.implementations.def.RabbitMQClientProvider;
import jakarta.inject.Singleton;
import lombok.Getter;
import lombok.extern.java.Log;

import java.lang.annotation.Annotation;

@Log
public class RabbitMQQueuePublisherProvider implements Provider<QueuePublisher>, IGuicePreDestroy<RabbitMQQueuePublisherProvider>
{
    private RabbitMQClientProvider clientProvider;
    @Getter
    private final QueueDefinition queueDefinition;
    private final String exchangeName;
    private final String routingKey;
    private QueuePublisher queuePublisher;
    private final boolean confirmPublish;

    public RabbitMQQueuePublisherProvider(RabbitMQClientProvider clientProvider, QueueDefinition queueDefinition, String exchangeName, String routingKey, boolean confirmPublish)
    {
        this.clientProvider = clientProvider;
        this.queueDefinition = queueDefinition;
        this.exchangeName = exchangeName;
        this.routingKey = routingKey;
        this.confirmPublish = confirmPublish;
    }

    public RabbitMQQueuePublisherProvider(RabbitMQClientProvider clientProvider, String queueDefinition, String exchangeName, String routingKey, boolean confirmPublish)
    {
        this.clientProvider = clientProvider;
        this.confirmPublish = confirmPublish;
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
        if(this.queuePublisher == null)
            queuePublisher = new QueuePublisher(clientProvider,confirmPublish, queueDefinition, exchangeName, routingKey);

        return queuePublisher;
    }

    @Override
    public void onDestroy()
    {
    }

    //before consumers and client
    @Override
    public Integer sortOrder()
    {
        return 20;
    }
}
