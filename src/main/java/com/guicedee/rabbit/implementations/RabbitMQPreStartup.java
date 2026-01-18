package com.guicedee.rabbit.implementations;

import com.google.common.base.Strings;
import com.google.inject.Key;
import com.google.inject.name.Names;
import com.guicedee.client.IGuiceContext;
import com.guicedee.client.services.lifecycle.IGuicePreStartup;
import com.guicedee.rabbit.*;
import com.guicedee.rabbit.implementations.def.QueueOptionsDefault;
import com.guicedee.vertx.spi.VertXPreStartup;
import com.guicedee.vertx.spi.VerticleBuilder;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import io.vertx.core.Future;
import com.google.inject.Inject;
import jakarta.inject.Named;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Pre-startup scanner that discovers RabbitMQ annotations and registers
 * exchanges, queues, publishers, and consumers for later binding.
 */
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
     * Returns queue consumer definitions limited to the exchanges mapped to the given package.
     *
     * @param packageName The package to filter by.
     * @return Matching queue definitions keyed by queue name.
     */
    public static Map<String, QueueDefinition> getQueueConsumerDefinitions(String packageName)
    {
        Map<String, QueueDefinition> packageLimitedQueueDefinitions = new HashMap<>();
        queueExchangeNames.forEach((queueName, exchangeName) -> {
            packageExchanges.forEach((pack,exchangeNam)->{
                if(exchangeName.equals(exchangeNam))
                {
                    packageLimitedQueueDefinitions.put(queueName, queueConsumerDefinitions.get(queueName));
                }
            });
        });
        return packageLimitedQueueDefinitions;
    }

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
     * Returns queue publisher definitions limited to the exchanges mapped to the given package.
     *
     * @param packageName The package to filter by.
     * @return Matching queue definitions keyed by queue name.
     */
    public static Map<String, QueueDefinition> getQueuePublisherDefinitions(String packageName)
    {
        Map<String, QueueDefinition> packageLimitedQueueDefinitions = new HashMap<>();
        queueExchangeNames.forEach((queueName, exchangeName) -> {
            packageExchanges.forEach((pack,exchangeNam)->{
                if(pack.equals(packageName) && exchangeName.equals(exchangeNam))
                {
                    packageLimitedQueueDefinitions.put(queueName, queuePublisherDefinitions.get(queueName));
                }
            });
        });
        return packageLimitedQueueDefinitions;
    }

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


    /**
     * Scans for RabbitMQ annotations and registers connection and queue metadata.
     *
     * @return Futures that complete once scanning has finished.
     */
    @Override
    public List<Future<Boolean>> onStartup()
    {
        return List.of(VertXPreStartup.getVertx().executeBlocking(() -> {
            ScanResult scanResult = IGuiceContext.instance().getScanResult();
            Set<Class<?>> completedConsumers = new HashSet<>();

            // Handle RabbitMQ client connections
            processClientConnections(scanResult, completedConsumers);

            return true;
        }));
    }

    /**
     * Handles the discovery and registration of client connections and related artifacts.
     */
    private void processClientConnections(ScanResult scanResult, Set<Class<?>> completedConsumers)
    {
        ClassInfoList clientConnections = scanResult.getClassesWithAnnotation(RabbitConnectionOptions.class);

        clientConnections.stream()
                .distinct()
                .filter(this::isVerticleBound)
                .forEach(clientConnection -> processClientConnection(scanResult, clientConnection, completedConsumers, false));
        clientConnections.stream()
                .distinct()
                .filter(this::isVerticleBound)
                .forEach(clientConnection -> processClientConnection(scanResult, clientConnection, completedConsumers, true));

        clientConnections.stream()
                .distinct()
                .filter(clientConnection -> !isVerticleBound(clientConnection))
                .forEach(clientConnection -> {
                    log.error("RabbitMQ Client Connection found but not bound to a declared verticle - {}", clientConnection.getName());
                });
    }

    /**
     * @return {@code true} when the annotated class is associated with a declared Verticle package.
     */
    private boolean isVerticleBound(ClassInfo clientConnection)
    {
        return VerticleBuilder.getVerticlePackages()
                .keySet().stream()
                .anyMatch(pkg -> clientConnection.getName().startsWith(pkg));
    }

    /**
     * Processes a single connection annotation and discovers exchanges and queues.
     */
    private void processClientConnection(ScanResult scanResult, ClassInfo clientConnection, Set<Class<?>> completedConsumers, boolean publishers)
    {
        log.debug("Found Verticle Bound RabbitMQ Connection - {}", clientConnection.getName());

        var connectionAnnotation = clientConnection.loadClass().getAnnotation(RabbitConnectionOptions.class);
        registerPackageConnection(clientConnection.getPackageName(), connectionAnnotation);

        var classInfos = scanResult.getPackageInfo(clientConnection.getPackageName()).getClassInfoRecursive();
        var exchanges = getExchanges(classInfos, clientConnection);

        for (ClassInfo exchange : exchanges)
        {
            processExchange(exchange, completedConsumers, publishers);
        }
    }

    /**
     * Registers connection options for a package if not already present.
     */
    private void registerPackageConnection(String packageName, RabbitConnectionOptions connectionAnnotation)
    {
        if (!packageRabbitMqConnections.containsKey(packageName))
            packageRabbitMqConnections.computeIfAbsent(packageName, k -> new ArrayList<>()).add(connectionAnnotation);
    }

    /**
     * Looks up exchange-annotated classes within the connection package.
     *
     * @param classInfos       Classes in the package tree.
     * @param clientConnection The connection annotation source.
     * @return All exchange declarations in the package.
     */
    private List<ClassInfo> getExchanges(List<ClassInfo> classInfos, ClassInfo clientConnection)
    {
        var exchanges = classInfos.stream()
                .filter(info -> info.hasAnnotation(QueueExchange.class))
                .distinct()
                .toList();

        if (exchanges.isEmpty())
        {
            throw new RuntimeException("No @QueueExchange Declared for Package - " + clientConnection.getPackageName());
        }
        return exchanges;
    }

    /**
     * Registers a discovered exchange and processes its consumers or publishers.
     */
    private void processExchange(ClassInfo exchange, Set<Class<?>> completedConsumers, boolean publishers)
    {
        String exchangeName = registerExchange(exchange);
        var exchangePackageClasses = exchange.getPackageInfo().getClassInfoRecursive();

        // Process Consumers
        if (!publishers)
            processExchangeConsumers(exchangePackageClasses, exchangeName, completedConsumers);

        // Process Publishers
        if (publishers)
            processExchangePublishers(exchangePackageClasses, exchangeName);
    }

    /**
     * Registers exchange metadata and returns its name.
     */
    private String registerExchange(ClassInfo exchange)
    {
        var ex = exchange.loadClass().getAnnotation(QueueExchange.class);
        String exchangeName = Strings.isNullOrEmpty(ex.value()) ? "default" : ex.value();

        packageExchanges.put(exchange.getPackageName(), exchangeName);
        exchangeDefinitions.put(exchangeName, ex);

        return exchangeName;
    }

    /**
     * Discovers and registers consumer classes within an exchange package.
     */
    private void processExchangeConsumers(List<ClassInfo> exchangePackageClasses, String exchangeName, Set<Class<?>> completedConsumers)
    {
        var queueConsumers = exchangePackageClasses.stream()
                .filter(info -> info.hasAnnotation(QueueDefinition.class))
                .distinct()
                .toList();

        for (ClassInfo consumerClassInfo : queueConsumers)
        {
            registerQueueConsumer(consumerClassInfo, exchangeName, completedConsumers);
        }
    }

    /**
     * Registers a queue consumer class and its routing metadata.
     */
    private void registerQueueConsumer(ClassInfo consumerClassInfo, String exchangeName, Set<Class<?>> completedConsumers)
    {
        Class<QueueConsumer> consumerClass = (Class<QueueConsumer>) consumerClassInfo.loadClass();

        if (completedConsumers.contains(consumerClass))
        {
            return;
        }
        completedConsumers.add(consumerClass);

        var queueDefinition = consumerClass.getAnnotation(QueueDefinition.class);
        String queueName = queueDefinition.value();

        if (!queueExchangeNames.containsKey(queueName))
            queueExchangeNames.put(queueName, exchangeName);

        registerConsumerQueue(queueName, exchangeName, queueDefinition, consumerClass);
    }

    /**
     * Discovers and registers queue publishers within an exchange package.
     */
    private void processExchangePublishers(List<ClassInfo> exchangePackageClasses, String exchangeName)
    {
        exchangePackageClasses.stream()
                .filter(info -> info.hasDeclaredFieldAnnotation(QueueDefinition.class) ||
                        info.getFieldInfo().stream()
                                .anyMatch(a -> a.getTypeSignatureOrTypeDescriptor().toString().equals("com.guicedee.rabbit.QueuePublisher")))
                .forEach(publisherClassInfo -> registerQueuePublisher(publisherClassInfo, exchangeName));
    }

    /**
     * Registers a publisher for a queue based on injected fields or class-level annotations.
     */
    private void registerQueuePublisher(ClassInfo publisherClassInfo, String exchangeName)
    {
        var fields = getPublisherField(publisherClassInfo);
        for (Field field : fields)
        {
            String queueName = getQueueNameFromField(field, publisherClassInfo);
            if (!queuePublisherDefinitions.containsKey(queueName))
            {
                String publisherExchange = queueExchangeNames.getOrDefault(queueName, exchangeName);
                var queueDefinition = field.getAnnotation(QueueDefinition.class);
                if (queueDefinition != null && !queueDefinition.exchange().equals("default"))
                {
                    publisherExchange = queueDefinition.exchange();
                } else
                {
                    publisherExchange = exchangeName;
                    if (queueDefinition == null)
                    {
                        String finalPublisherExchange = publisherExchange;
                        queueDefinition = new QueueDefinition()
                        {
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
                registerPublisherQueue(queueName, exchangeName, queueDefinition, false);
                log.debug("Found Queue Publisher - {} - {} - {}", queueName, publisherExchange, queueRoutingKeys.get(queueName));
            }
        }
    }

    /**
     * Resolves the queue name for a publisher field.
     *
     * @return The queue name, or {@code null} if no annotation is present.
     */
    private String getQueueNameFromField(Field field, ClassInfo publisherClassInfo)
    {
        if (field.isAnnotationPresent(Named.class))
        {
            return field.getAnnotation(Named.class).value();
        } else if (field.isAnnotationPresent(com.google.inject.name.Named.class))
        {
            return field.getAnnotation(com.google.inject.name.Named.class).value();
        } else
        {
            var queueDefinition = publisherClassInfo.loadClass().getAnnotation(QueueDefinition.class);
            return queueDefinition != null ? queueDefinition.value() : null;
        }
    }

    /**
     * Registers the consumer metadata and routing key, and ensures publisher metadata exists.
     */
    private void registerConsumerQueue(String queueName, String exchangeName, QueueDefinition queueDefinition, Class<QueueConsumer> aClass)
    {
        queueConsumerDefinitions.put(queueName, queueDefinition);
        queueConsumerClass.put(queueName, aClass);
        queueConsumerKeys.put(queueName, Key.get(aClass, Names.named(queueName)));

        String routingKey = exchangeName + "_" + queueDefinition.value();
        queueRoutingKeys.put(queueName, routingKey);

        log.debug("Found Queue Consumer - {} - {} - {}", queueName, exchangeName, routingKey);

        registerPublisherQueue(queueName, exchangeName, queueDefinition, true);
    }

    /**
     * Registers publisher metadata for the queue, optionally overriding existing values.
     *
     * @param queueName       The queue name to register.
     * @param exchangeName    The exchange used to build the routing key.
     * @param queueDefinition The queue definition to register.
     * @param override        Whether to replace existing publisher data.
     */
    private void registerPublisherQueue(String queueName, String exchangeName, QueueDefinition queueDefinition, boolean override)
    {
        if (!queuePublisherDefinitions.containsKey(queueName) || override)
        {
            queuePublisherDefinitions.put(queueName, queueDefinition);
            queuePublisherKeys.put(queueName, Key.get(QueuePublisher.class, Names.named(queueName)));
        }
        if (!queueRoutingKeys.containsKey(queueName) || override)
        {
            String routingKey = exchangeName + "_" + queueDefinition.value();
            queueRoutingKeys.put(queueName, routingKey);
        }
        if (!queueExchangeNames.containsKey(queueName))
        {
            queueExchangeNames.put(queueName, exchangeName);
        }
    }

    /**
     * Ensures this startup step runs early in the lifecycle.
     *
     * @return Ordering value for the pre-startup hook.
     */
    @Override
    public Integer sortOrder()
    {
        return Integer.MIN_VALUE + 80;
    }

    /**
     * Finds injectable {@link QueuePublisher} fields that are named and not static/final.
     *
     * @param aClass The class to inspect.
     * @return A list of matching fields.
     */
    private List<Field> getPublisherField(ClassInfo aClass)
    {
        // Load the class once to avoid multiple calls
        Class<?> clazz = aClass.loadClass();
        return Arrays.stream(clazz.getDeclaredFields())  // Stream the declared fields
                .filter(field -> !isFinal(field) && !isStatic(field)) // Filter out final and static fields
                .filter(field -> {
                    // Check annotation presence
                    boolean hasInject = field.isAnnotationPresent(Inject.class) || field.isAnnotationPresent(com.google.inject.Inject.class);
                    boolean hasNamed = field.isAnnotationPresent(Named.class) || field.isAnnotationPresent(com.google.inject.name.Named.class);
                    return (hasInject && hasNamed) && (field.getType().equals(QueuePublisher.class));
                })
                .map(field -> {
                    try
                    {
                        // Attempt to re-fetch the field (if required)
                        return clazz.getDeclaredField(field.getName());
                    } catch (NoSuchFieldException | IllegalAccessError e)
                    {
                        // Log exception if necessary
                        log.debug("Field {} could not be loaded: {}", field.getName(), e.getMessage());
                        return null; // Null signals skipping this entry
                    }
                })
                .filter(Objects::nonNull) // Remove null entries due to exceptions
                .collect(Collectors.toList()); // Collect to the list
    }

    /**
     * @return {@code true} if the field is final.
     */
    public static boolean isFinal(Field field)
    {
        return Modifier.isFinal(field.getModifiers());
    }

    /**
     * @return {@code true} if the field is static.
     */
    public static boolean isStatic(Field field)
    {
        return Modifier.isStatic(field.getModifiers());
    }


}
