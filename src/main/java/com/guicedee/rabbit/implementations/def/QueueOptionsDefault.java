package com.guicedee.rabbit.implementations.def;

import com.guicedee.rabbit.QueueOptions;
import lombok.Setter;

import java.lang.annotation.Annotation;

@Setter
public class QueueOptionsDefault implements QueueOptions
{
    private int priority;
    private int fetchCount;
    private boolean durable;
    private boolean delete;
    private boolean autoAck = true;
    private boolean consumerExclusive;
    private boolean singleConsumer;
    private int ttl;
    private int maxInternalQueueSize = Integer.MAX_VALUE;
    private boolean noLocal;
    private boolean keepMostRecent = true;
    
    @Override
    public int priority()
    {
        return priority;
    }

    @Override
    public int fetchCount()
    {
        return fetchCount;
    }

    @Override
    public boolean durable()
    {
        return durable;
    }

    @Override
    public boolean delete()
    {
        return delete;
    }

    @Override
    public boolean autoAck()
    {
        return autoAck;
    }

    @Override
    public boolean consumerExclusive()
    {
        return consumerExclusive;
    }

    @Override
    public boolean singleConsumer()
    {
        return singleConsumer;
    }

    @Override
    public int ttl()
    {
        return ttl;
    }

    @Override
    public boolean noLocal()
    {
        return noLocal;
    }

    @Override
    public boolean keepMostRecent()
    {
        return keepMostRecent;
    }

    @Override
    public int maxInternalQueueSize()
    {
        return maxInternalQueueSize;
    }

    @Override
    public Class<? extends Annotation> annotationType()
    {
        return QueueOptions.class;
    }

    @Override
    public boolean equals(Object obj)
    {
        return true;
    }

    @Override
    public int hashCode()
    {
        return "0".hashCode();
    }
}
