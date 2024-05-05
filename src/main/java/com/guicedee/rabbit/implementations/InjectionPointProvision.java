package com.guicedee.rabbit.implementations;

import com.google.inject.Inject;
import com.google.inject.InjectionPointProvider;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;

public class InjectionPointProvision implements InjectionPointProvider
{
    @Override
    public Class<? extends Annotation> injectionPoint(AnnotatedElement member)
    {
        return Inject.class;
    }
}
