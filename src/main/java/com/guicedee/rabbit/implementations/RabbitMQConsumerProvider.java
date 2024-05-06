package com.guicedee.rabbit.implementations;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.guicedee.client.IGuiceContext;
import com.guicedee.rabbit.QueueConsumer;
import com.guicedee.rabbit.QueueDefinition;
import io.vertx.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQConsumer;
import jakarta.inject.Singleton;
import lombok.extern.java.Log;

import java.util.logging.Level;

import static com.guicedee.rabbit.implementations.RabbitMQClientProvider.startQueueFuture;
import static com.guicedee.rabbit.implementations.RabbitPostStartup.toOptions;

@Log
@Singleton
public class RabbitMQConsumerProvider implements Provider<QueueConsumer>
{
    @Inject
    private RabbitMQClient client;

    private final QueueDefinition queueDefinition;
    private final Class<QueueConsumer> clazz;

    private QueueConsumer queueConsumer = null;
    private RabbitMQConsumer consumer = null;

    @SuppressWarnings("unchecked")
    public RabbitMQConsumerProvider(QueueDefinition queueDefinition, Class clazz)
    {
        this.queueDefinition = queueDefinition;
        this.clazz = clazz;
    }

    @Override
    public QueueConsumer get()
    {
        startQueueFuture.andThen(a -> {
            if (queueConsumer == null || consumer.isCancelled())
            {
                if (!client.isConnected())
                {
                    client.addConnectionEstablishedCallback((connectionEstablished) -> {
                        createConsumer();
                    });
                }
                else
                {
                    createConsumer();
                }
            }
        });
      /*  if(queueConsumer == null)
            for (int i = 0; i < 5; i++)
            {
                try
                {
                    TimeUnit.SECONDS.sleep(1);
                    if (queueConsumer != null)
                    {
                        break;
                    }
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            }*/
        //request class and inject
        return queueConsumer;
    }

    private void createConsumer()
    {

        client.basicConsumer(queueDefinition.value(), toOptions(this.queueDefinition.options()), (event) -> {
            if (event.succeeded())
            {
                consumer = event.result();
                consumer.setQueueName(queueDefinition.value());
                consumer = consumer.fetch(queueDefinition.options()
                                                         .fetchCount());
                try
                {
                    queueConsumer = clazz.newInstance();
                    IGuiceContext.instance()
                                 .inject()
                                 .injectMembers(queueConsumer);
                }
                catch (InstantiationException | IllegalAccessException e)
                {
                    throw new RuntimeException(e);
                }
                consumer.handler((message) -> {
                    try
                    {
                        queueConsumer.consume(message);
                    }
                    catch (Throwable e)
                    {
                        throw new RuntimeException(e);
                    }
                });
            }
            else
            {
                log.log(Level.SEVERE, "Could not bind rabbit mq consumer on queue [" + queueDefinition.value() + "]", event.cause());
            }
        });
    }

    public void pause()
    {
        consumer.pause();
    }

    public void resume()
    {
        consumer.resume();
    }
}
