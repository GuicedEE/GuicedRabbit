package com.guicedee.rabbit.implementations;

import com.google.common.base.Strings;
import com.google.inject.Key;
import com.google.inject.name.Names;
import com.guicedee.client.IGuiceContext;
import com.guicedee.guicedinjection.interfaces.IGuicePreStartup;
import com.guicedee.rabbit.*;
import com.guicedee.rabbit.implementations.def.QueueOptionsDefault;
import com.guicedee.vertx.spi.VertXPreStartup;
import com.guicedee.vertx.spi.VerticleBuilder;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import io.vertx.core.Future;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.stream.Collectors;

@Log4j2
public class RabbitMQPreStartup implements IGuicePreStartup<RabbitMQPreStartup>
{
    /**
     * Root package declarations per connection
     */
    @Getter
    private static final Map<String, List<RabbitConnectionOptions>> packageRabbitMqConnections = new TreeMap<>();

    /**
     * Exchange list definitions
     */
    @Getter
    private static final Map<String, QueueExchange> exchangeDefinitions = new HashMap<>();

    /**
     * Queue name definitions
     */
    @Getter
    private static final Map<String, QueueDefinition> queueConsumerDefinitions = new HashMap<>();

    /**
     * Queue name definitions
     */
    @Getter
    private static final Map<String, Class<? extends QueueConsumer>> queueConsumerClass = new HashMap<>();

    /**
     * Queue name definitions
     */
    @Getter
    private static final Map<String, Key<?>> queueConsumerKeys = new HashMap<>();
    /**
     * Queue publisher name definitions
     */
    @Getter
    private static final Map<String, QueueDefinition> queuePublisherDefinitions = new HashMap<>();
    /**
     * A list of injection keys per publisher
     */
    @Getter
    private static final Map<String, Key<?>> queuePublisherKeys = new HashMap<>();

    /**
     * Package name to exchange
     */
    @Getter
    private static final Map<String, String> packageExchanges = new HashMap<>();

    /**
     * Queue name to exchange name
     */
    @Getter
    private static final Map<String, String> queueExchangeNames = new HashMap<>();

    /**
     * A list of specific routing keys assigned for a queue name
     */
    @Getter
    private static final Map<String, String> queueRoutingKeys = new HashMap<>();


    @Override
    public List<Future<Boolean>> onStartup() {
        return List.of(VertXPreStartup.getVertx().executeBlocking(() -> {
            ScanResult scanResult = IGuiceContext.instance().getScanResult();
            Set<Class<?>> completedConsumers = new HashSet<>();

            // Handle RabbitMQ client connections
            processClientConnections(scanResult, completedConsumers);

            return true;
        }));
    }

    private void processClientConnections(ScanResult scanResult, Set<Class<?>> completedConsumers) {
        ClassInfoList clientConnections = scanResult.getClassesWithAnnotation(RabbitConnectionOptions.class);

        clientConnections.stream()
                .distinct()
                .filter(clientConnection -> isVerticleBound(clientConnection))
                .forEach(clientConnection -> processClientConnection(scanResult, clientConnection, completedConsumers));

        clientConnections.stream()
                .distinct()
                .filter(clientConnection -> !isVerticleBound(clientConnection))
                .forEach(clientConnection -> {
                    log.error("RabbitMQ Client Connection found but not bound to a declared verticle - {}", clientConnection.getName());
                });
    }

    private boolean isVerticleBound(ClassInfo clientConnection) {
        return VerticleBuilder.getVerticlePackages()
                .keySet().stream()
                .anyMatch(pkg -> clientConnection.getName().startsWith(pkg));
    }

    private void processClientConnection(ScanResult scanResult, ClassInfo clientConnection, Set<Class<?>> completedConsumers) {
        log.info("Found Verticle Bound RabbitMQ Connection - {}", clientConnection.getName());

        var connectionAnnotation = clientConnection.loadClass().getAnnotation(RabbitConnectionOptions.class);
        registerPackageConnection(clientConnection.getPackageName(), connectionAnnotation);

        var classInfos = scanResult.getPackageInfo(clientConnection.getPackageName()).getClassInfoRecursive();
        var exchanges = getExchanges(classInfos, clientConnection);

        for (ClassInfo exchange : exchanges) {
            processExchange(exchange, completedConsumers);
        }
    }

    private void registerPackageConnection(String packageName, RabbitConnectionOptions connectionAnnotation) {
        packageRabbitMqConnections.computeIfAbsent(packageName, k -> new ArrayList<>()).add(connectionAnnotation);
    }

    private List<ClassInfo> getExchanges(List<ClassInfo> classInfos, ClassInfo clientConnection) {
        var exchanges = classInfos.stream()
                .filter(info -> info.hasAnnotation(QueueExchange.class))
                .distinct()
                .toList();

        if (exchanges.isEmpty()) {
            throw new RuntimeException("No @QueueExchange Declared for Package - " + clientConnection.getPackageName());
        }
        return exchanges;
    }

    private void processExchange(ClassInfo exchange, Set<Class<?>> completedConsumers) {
        String exchangeName = registerExchange(exchange);
        var exchangePackageClasses = exchange.getPackageInfo().getClassInfoRecursive();

        // Process Consumers
        processExchangeConsumers(exchangePackageClasses, exchangeName, completedConsumers);

        // Process Publishers
        processExchangePublishers(exchangePackageClasses, exchangeName);
    }

    private String registerExchange(ClassInfo exchange) {
        var ex = exchange.loadClass().getAnnotation(QueueExchange.class);
        String exchangeName = Strings.isNullOrEmpty(ex.value()) ? "default" : ex.value();

        packageExchanges.put(exchange.getPackageName(), exchangeName);
        exchangeDefinitions.put(exchangeName, ex);

        return exchangeName;
    }

    private void processExchangeConsumers(List<ClassInfo> exchangePackageClasses, String exchangeName, Set<Class<?>> completedConsumers) {
        var queueConsumers = exchangePackageClasses.stream()
                .filter(info -> info.hasAnnotation(QueueDefinition.class))
                .distinct()
                .toList();

        for (ClassInfo consumerClassInfo : queueConsumers) {
            registerQueueConsumer(consumerClassInfo, exchangeName, completedConsumers);
        }
    }

    private void registerQueueConsumer(ClassInfo consumerClassInfo, String exchangeName, Set<Class<?>> completedConsumers) {
        Class<QueueConsumer> consumerClass = (Class<QueueConsumer>) consumerClassInfo.loadClass();

        if (completedConsumers.contains(consumerClass)) {
            return;
        }
        completedConsumers.add(consumerClass);

        var queueDefinition = consumerClass.getAnnotation(QueueDefinition.class);
        String queueName = queueDefinition.value();

        queueExchangeNames.put(queueName, exchangeName);
        registerConsumerQueue(queueName, exchangeName, queueDefinition, consumerClass);

        String routingKey = exchangeName + "_" + queueDefinition.value();
        queueRoutingKeys.put(queueName, routingKey);

        log.info("Found Queue Consumer - {} - {} - {}", queueName, exchangeName, routingKey);
    }

    private void processExchangePublishers(List<ClassInfo> exchangePackageClasses, String exchangeName) {
        exchangePackageClasses.stream()
                .filter(info -> info.hasDeclaredFieldAnnotation(QueueDefinition.class) ||
                        info.getFieldInfo().stream()
                                .anyMatch(a->a.getTypeSignatureOrTypeDescriptor().toString().equals("com.guicedee.rabbit.QueuePublisher")))
                .forEach(publisherClassInfo -> registerQueuePublisher(publisherClassInfo, exchangeName));
    }

    private void registerQueuePublisher(ClassInfo publisherClassInfo, String exchangeName) {
        var fields = getPublisherField(publisherClassInfo);
        for (Field field : fields) {
            String queueName = getQueueNameFromField(field, publisherClassInfo);
            if (!queuePublisherDefinitions.containsKey(queueName)) {
                String publisherExchange = queueExchangeNames.getOrDefault(queueName, exchangeName);
                var queueDefinition = field.getAnnotation(QueueDefinition.class);
                if (queueDefinition != null && !queueDefinition.exchange().equals("default")) {
                    publisherExchange = queueDefinition.exchange();
                }else {
                    publisherExchange = exchangeName;
                    if (queueDefinition == null)
                    {
                        String finalPublisherExchange = publisherExchange;
                        queueDefinition = new QueueDefinition(){
                            @Override
                            public Class<? extends Annotation> annotationType()
                            {
                                return QueueExchange.class;
                            }

                            @Override
                            public String value()
                            {
                                return queueName;
                            }

                            @Override
                            public QueueOptions options()
                            {
                                return new QueueOptionsDefault();
                            }

                            @Override
                            public String exchange()
                            {
                                return finalPublisherExchange;
                            }
                        };
                    }
                }
                registerPublisherQueue(queueName, exchangeName, queueDefinition,false);
                if(!queueRoutingKeys.containsKey(queueName))
                {
                    String routingKey = exchangeName + "_" + queueDefinition.value();
                    queueRoutingKeys.put(queueName, routingKey);
                }
                log.info("Found Queue Publisher - {} - {} - {}", queueName, publisherExchange, queueRoutingKeys.get(queueName));
            }
        }
    }

    private String getQueueNameFromField(Field field, ClassInfo publisherClassInfo) {
        if (field.isAnnotationPresent(Named.class)) {
            return field.getAnnotation(Named.class).value();
        } else if (field.isAnnotationPresent(com.google.inject.name.Named.class)) {
            return field.getAnnotation(com.google.inject.name.Named.class).value();
        } else {
            var queueDefinition = publisherClassInfo.loadClass().getAnnotation(QueueDefinition.class);
            return queueDefinition != null ? queueDefinition.value() : null;
        }
    }

    private void registerConsumerQueue(String queueName, String exchangeName, QueueDefinition queueDefinition, Class<QueueConsumer> aClass)
    {
        queueConsumerDefinitions.put(queueName, queueDefinition);
        queueConsumerClass.put(queueName, aClass);
        queueConsumerKeys.put(queueName, Key.get(aClass, Names.named(queueName)));
        registerPublisherQueue(queueName,exchangeName,queueDefinition,true);
    }

    /**
     *
     * @param queueName
     * @param queueDefinition
     * @param override if it comes from a consumer definition
     */
    private void registerPublisherQueue(String queueName, String exchangeName, QueueDefinition queueDefinition, boolean override)
    {
        if(!queuePublisherDefinitions.containsKey(queueName) || override)
        {
            queuePublisherDefinitions.put(queueName, queueDefinition);
            queuePublisherKeys.put(queueName, Key.get(QueuePublisher.class, Names.named(queueName)));
        }
    }

    @Override
    public Integer sortOrder()
    {
        return Integer.MIN_VALUE + 80;
    }

    private List<Field> getPublisherField(ClassInfo aClass) {
        // Load the class once to avoid multiple calls
        Class<?> clazz = aClass.loadClass();
        return Arrays.stream(clazz.getDeclaredFields())  // Stream the declared fields
                .filter(field -> !isFinal(field) && !isStatic(field)) // Filter out final and static fields
                .filter(field -> {
                    // Check annotation presence
                    boolean hasInject = field.isAnnotationPresent(Inject.class) || field.isAnnotationPresent(com.google.inject.Inject.class);
                    boolean hasNamed = field.isAnnotationPresent(Named.class) || field.isAnnotationPresent(com.google.inject.name.Named.class);
                    return (hasInject && hasNamed) || field.getType().equals(QueuePublisher.class);
                })
                .map(field -> {
                    try {
                        // Attempt to re-fetch the field (if required)
                        return clazz.getDeclaredField(field.getName());
                    } catch (NoSuchFieldException | IllegalAccessError e) {
                        // Log exception if necessary
                        log.debug("Field {} could not be loaded: {}", field.getName(), e.getMessage());
                        return null; // Null signals skipping this entry
                    }
                })
                .filter(Objects::nonNull) // Remove null entries due to exceptions
                .collect(Collectors.toList()); // Collect to the list
    }

    public static boolean isFinal(Field field)
    {
        return Modifier.isFinal(field.getModifiers());
    }

    public static boolean isStatic(Field field)
    {
        return Modifier.isStatic(field.getModifiers());
    }


}
