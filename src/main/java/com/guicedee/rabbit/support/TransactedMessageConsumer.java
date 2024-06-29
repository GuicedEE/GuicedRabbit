package com.guicedee.rabbit.support;

import com.guicedee.client.IGuiceContext;
import com.guicedee.rabbit.QueueConsumer;
import io.vertx.rabbitmq.RabbitMQMessage;
import jakarta.transaction.Transactional;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TransactedMessageConsumer
{
    private QueueConsumer queueConsumer;

    @Transactional
    public void execute(Class<? extends QueueConsumer> clazz, RabbitMQMessage message)
    {
        if (queueConsumer == null)
        {
            queueConsumer = IGuiceContext.get(clazz);
        }
        queueConsumer.consume(message);
    }
}
