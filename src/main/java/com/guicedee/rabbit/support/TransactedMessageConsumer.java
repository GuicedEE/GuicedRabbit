package com.guicedee.rabbit.support;

import com.google.inject.Key;
import com.google.inject.name.Names;
import com.guicedee.client.IGuiceContext;
import com.guicedee.rabbit.QueueConsumer;
import com.guicedee.rabbit.QueueDefinition;
import io.vertx.rabbitmq.RabbitMQMessage;
import jakarta.transaction.Transactional;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TransactedMessageConsumer
{
    private QueueConsumer queueConsumer;
    private Class<? extends QueueConsumer> clazz;
    private QueueDefinition queueDefinition;

    @Transactional
    public void execute(Class<? extends QueueConsumer> clazz, RabbitMQMessage message)
    {
        if (queueConsumer == null)
        {
            Key<QueueConsumer> queueConsumerKey = (Key<QueueConsumer>) Key.get(clazz, Names.named(queueDefinition.value()));
            queueConsumer = IGuiceContext.get(queueConsumerKey);
        }
        queueConsumer.consume(message);
    }
}
