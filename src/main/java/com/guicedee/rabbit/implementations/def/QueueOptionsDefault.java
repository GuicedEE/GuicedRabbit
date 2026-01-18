package com.guicedee.rabbit.implementations.def;

import com.guicedee.rabbit.QueueOptions;
import lombok.Setter;

import java.lang.annotation.Annotation;

/**
 * Default, mutable implementation of {@link QueueOptions} used when no annotation is present.
 */
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
    private boolean transacted = true;
    private boolean autobind = true;

    /**
     * @return Max priority value for the queue.
     */
    @Override
    public int priority()
    {
        return priority;
    }

    /**
     * @return Prefetch count for the consumer.
     */
    @Override
    public int fetchCount()
    {
        return fetchCount;
    }

    /**
     * @return Whether the queue is durable.
     */
    @Override
    public boolean durable()
    {
        return durable;
    }

    /**
     * @return Whether the queue is auto-deleted.
     */
    @Override
    public boolean delete()
    {
        return delete;
    }

    /**
     * @return Whether auto-ack is enabled.
     */
    @Override
    public boolean autoAck()
    {
        return autoAck;
    }

    /**
     * @return Whether the queue is exclusive to the consumer.
     */
    @Override
    public boolean consumerExclusive()
    {
        return consumerExclusive;
    }

    /**
     * @return Whether only a single consumer is allowed.
     */
    @Override
    public boolean singleConsumer()
    {
        return singleConsumer;
    }

    /**
     * @return Message time-to-live in milliseconds.
     */
    @Override
    public int ttl()
    {
        return ttl;
    }

    /**
     * @return Whether messages should not be delivered to the same connection.
     */
    @Override
    public boolean noLocal()
    {
        return noLocal;
    }

    /**
     * @return Whether only the most recent messages are kept.
     */
    @Override
    public boolean keepMostRecent()
    {
        return keepMostRecent;
    }

    /**
     * @return Max internal queue size for the consumer buffer.
     */
    @Override
    public int maxInternalQueueSize()
    {
        return maxInternalQueueSize;
    }

    /**
     * @return The annotation type for this proxy implementation.
     */
    @Override
    public Class<? extends Annotation> annotationType()
    {
        return QueueOptions.class;
    }

    /**
     * Returns {@code true} to align with annotation semantics.
     *
     * @return Always {@code true}.
     */
    @Override
    public boolean equals(Object obj)
    {
        return true;
    }

    /**
     * @return A stable hash code for annotation compatibility.
     */
    @Override
    public int hashCode()
    {
        return "0".hashCode();
    }

    /**
     * @return Whether message handling should be transactional.
     */
    public boolean transacted()
    {
        return transacted;
    }

    /**
     * @return Whether consumers should auto-bind at startup.
     */
    @Override
    public boolean autobind()
    {
        return autobind;
    }

    /**
     * @return Number of consumer instances to create.
     */
    @Override
    public int consumerCount() {
        return 1;
    }

    /**
     * @return Whether to use a dedicated channel.
     */
    @Override
    public boolean dedicatedChannel() {
        return false;
    }

    /**
     * @return Whether to use a dedicated connection.
     */
    @Override
    public boolean dedicatedConnection() {
        return false;
    }
}
