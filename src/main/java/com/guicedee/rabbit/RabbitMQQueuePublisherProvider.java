package com.guicedee.rabbit;

import com.google.inject.Provider;
import com.guicedee.guicedinjection.interfaces.IGuicePreDestroy;
import com.guicedee.rabbit.implementations.def.QueueOptionsDefault;
import jakarta.inject.Singleton;
import lombok.extern.java.Log;

import java.lang.annotation.Annotation;

@Singleton
@Log
public class RabbitMQQueuePublisherProvider implements Provider<QueuePublisher>, IGuicePreDestroy<RabbitMQQueuePublisherProvider>
{
    private final QueueDefinition queueDefinition;
    private final String exchangeName;
    private final String routingKey;

    private QueuePublisher queuePublisher;

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
        if(this.queuePublisher == null)
            queuePublisher = new QueuePublisher(queueDefinition, exchangeName, routingKey);

        return queuePublisher;
    }

    @Override
    public void onDestroy()
    {
        if (queuePublisher != null)
        {
            log.config("Pausing queue publisher - " + this.routingKey);
            queuePublisher.pause();
        }
    }

    //before consumers and client
    @Override
    public Integer sortOrder()
    {
        return 20;
    }
}
