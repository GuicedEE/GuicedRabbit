package com.guicedee.rabbit.implementations.def;

import com.guicedee.rabbit.Queue;
import com.guicedee.rabbit.QueueOptions;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.lang.annotation.Annotation;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class QueueDefault implements Queue
{
    private String value;
    private String exchange;
    private QueueOptions options = new QueueOptionsDefault();


    @Override
    public String value()
    {
        return value;
    }

    @Override
    public QueueOptions options()
    {
        return options;
    }

    @Override
    public String exchange()
    {
        return exchange;
    }

    @Override
    public Class<? extends Annotation> annotationType()
    {
        return Queue.class;
    }
}
