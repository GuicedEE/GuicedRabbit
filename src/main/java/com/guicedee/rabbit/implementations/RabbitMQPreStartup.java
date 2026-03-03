package com.guicedee.rabbit.implementations;

import com.google.common.base.Strings;
import com.google.inject.Key;
import com.google.inject.name.Names;
import com.guicedee.client.IGuiceContext;
import com.guicedee.client.services.lifecycle.IGuicePreStartup;
import com.guicedee.rabbit.*;
import com.guicedee.rabbit.implementations.def.QueueOptionsDefault;
import com.guicedee.vertx.spi.VertXPreStartup;
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
                .forEach(clientConnection -> processClientConnection(scanResult, clientConnection, completedConsumers, false));
        clientConnections.stream()
                .distinct()
                .forEach(clientConnection -> processClientConnection(scanResult, clientConnection, completedConsumers, true));
    }

    /**
     * Processes a single connection annotation and discovers exchanges and queues.
     */
    private void processClientConnection(ScanResult scanResult, ClassInfo clientConnection, Set<Class<?>> completedConsumers, boolean publishers)
    {
        log.debug("Found RabbitMQ Connection - {}", clientConnection.getName());

        var connectionAnnotation = clientConnection.loadClass().getAnnotation(RabbitConnectionOptions.class);
        String connectionName = connectionAnnotation.value();
        RabbitConnectionOptions wrappedConnectionOptions = new RabbitConnectionOptions()
        {
            @Override
            public Class<? extends Annotation> annotationType()
            {
                return RabbitConnectionOptions.class;
            }

            @Override
            public String value()
            {
                return envForName(connectionName, "CONNECTION_NAME", connectionAnnotation.value());
            }

            @Override
            public String uri()
            {
                return envForName(connectionName, "URI", connectionAnnotation.uri());
            }

            @Override
            public String[] addresses()
            {
                String addrStr = envForName(connectionName, "ADDRESSES", String.join(",", connectionAnnotation.addresses()));
                return addrStr.isBlank() ? new String[0] : addrStr.split(",");
            }

            @Override
            public String user()
            {
                return envForName(connectionName, "USER", connectionAnnotation.user());
            }

            @Override
            public String password()
            {
                return envForName(connectionName, "PASSWORD", connectionAnnotation.password());
            }

            @Override
            public String host()
            {
                return envForName(connectionName, "HOST", connectionAnnotation.host());
            }

            @Override
            public String virtualHost()
            {
                return envForName(connectionName, "VIRTUAL_HOST", connectionAnnotation.virtualHost());
            }

            @Override
            public int port()
            {
                return Integer.parseInt(envForName(connectionName, "PORT", String.valueOf(connectionAnnotation.port())));
            }

            @Override
            public int connectionTimeout()
            {
                return Integer.parseInt(envForName(connectionName, "CONNECTION_TIMEOUT", String.valueOf(connectionAnnotation.connectionTimeout())));
            }

            @Override
            public int requestedHeartbeat()
            {
                return Integer.parseInt(envForName(connectionName, "REQUESTED_HEARTBEAT", String.valueOf(connectionAnnotation.requestedHeartbeat())));
            }

            @Override
            public int handshakeTimeout()
            {
                return Integer.parseInt(envForName(connectionName, "HANDSHAKE_TIMEOUT", String.valueOf(connectionAnnotation.handshakeTimeout())));
            }

            @Override
            public int requestedChannelMax()
            {
                return Integer.parseInt(envForName(connectionName, "REQUESTED_CHANNEL_MAX", String.valueOf(connectionAnnotation.requestedChannelMax())));
            }

            @Override
            public long networkRecoveryInterval()
            {
                return Long.parseLong(envForName(connectionName, "NETWORK_RECOVERY_INTERVAL", String.valueOf(connectionAnnotation.networkRecoveryInterval())));
            }

            @Override
            public boolean automaticRecoveryEnabled()
            {
                return Boolean.parseBoolean(envForName(connectionName, "AUTOMATIC_RECOVERY_ENABLED", String.valueOf(connectionAnnotation.automaticRecoveryEnabled())));
            }

            @Override
            public boolean automaticRecoveryOnInitialConnection()
            {
                return Boolean.parseBoolean(envForName(connectionName, "AUTOMATIC_RECOVERY_ON_INITIAL_CONNECTION", String.valueOf(connectionAnnotation.automaticRecoveryOnInitialConnection())));
            }

            @Override
            public boolean includeProperties()
            {
                return Boolean.parseBoolean(envForName(connectionName, "INCLUDE_PROPERTIES", String.valueOf(connectionAnnotation.includeProperties())));
            }

            @Override
            public boolean useNio()
            {
                return Boolean.parseBoolean(envForName(connectionName, "USE_NIO", String.valueOf(connectionAnnotation.useNio())));
            }

            @Override
            public int reconnectAttempts()
            {
                return Integer.parseInt(envForName(connectionName, "RECONNECT_ATTEMPTS", String.valueOf(connectionAnnotation.reconnectAttempts())));
            }

            @Override
            public long reconnectInterval()
            {
                return Long.parseLong(envForName(connectionName, "RECONNECT_INTERVAL", String.valueOf(connectionAnnotation.reconnectInterval())));
            }

            @Override
            public String hostnameVerificationAlgorithm()
            {
                return envForName(connectionName, "HOSTNAME_VERIFICATION_ALGORITHM", connectionAnnotation.hostnameVerificationAlgorithm());
            }

            @Override
            public String[] applicationLayerProtocols()
            {
                String protocolsStr = envForName(connectionName, "APPLICATION_LAYER_PROTOCOLS", String.join(",", connectionAnnotation.applicationLayerProtocols()));
                return protocolsStr.isBlank() ? new String[0] : protocolsStr.split(",");
            }

            @Override
            public boolean registerWriteHandler()
            {
                return Boolean.parseBoolean(envForName(connectionName, "REGISTER_WRITE_HANDLER", String.valueOf(connectionAnnotation.registerWriteHandler())));
            }

            @Override
            public boolean confirmPublishes()
            {
                return Boolean.parseBoolean(envForName(connectionName, "CONFIRM_PUBLISHES", String.valueOf(connectionAnnotation.confirmPublishes())));
            }
        };
        registerPackageConnection(clientConnection.getPackageName(), wrappedConnectionOptions);

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
        String rawExchangeName = ex.value();
        QueueExchange wrappedExchange = new QueueExchange()
        {
            @Override
            public Class<? extends Annotation> annotationType()
            {
                return QueueExchange.class;
            }

            @Override
            public String value()
            {
                return envForName(rawExchangeName, "EXCHANGE_NAME", ex.value());
            }

            @Override
            public boolean createDeadLetter()
            {
                return Boolean.parseBoolean(envForName(rawExchangeName, "EXCHANGE_CREATE_DEAD_LETTER", String.valueOf(ex.createDeadLetter())));
            }

            @Override
            public boolean durable()
            {
                return Boolean.parseBoolean(envForName(rawExchangeName, "EXCHANGE_DURABLE", String.valueOf(ex.durable())));
            }

            @Override
            public boolean autoDelete()
            {
                return Boolean.parseBoolean(envForName(rawExchangeName, "EXCHANGE_AUTO_DELETE", String.valueOf(ex.autoDelete())));
            }

            @Override
            public ExchangeType exchangeType()
            {
                String typeStr = envForName(rawExchangeName, "EXCHANGE_TYPE", ex.exchangeType().name());
                try
                {
                    return ExchangeType.valueOf(typeStr);
                }
                catch (IllegalArgumentException e)
                {
                    return ex.exchangeType();
                }
            }
        };
        String exchangeName = Strings.isNullOrEmpty(wrappedExchange.value()) ? "default" : wrappedExchange.value();

        packageExchanges.put(exchange.getPackageName(), exchangeName);
        exchangeDefinitions.put(exchangeName, wrappedExchange);

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
        QueueDefinition wrappedQueueDefinition = wrapQueueDefinition(queueDefinition);
        String queueName = wrappedQueueDefinition.value();

        if (!queueExchangeNames.containsKey(queueName))
            queueExchangeNames.put(queueName, exchangeName);

        registerConsumerQueue(queueName, exchangeName, wrappedQueueDefinition, consumerClass);
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
                QueueDefinition wrappedQueueDefinition = null;
                if (queueDefinition != null && !queueDefinition.exchange().equals("default"))
                {
                    publisherExchange = queueDefinition.exchange();
                    wrappedQueueDefinition = wrapQueueDefinition(queueDefinition);
                } else
                {
                    publisherExchange = exchangeName;
                    if (queueDefinition == null)
                    {
                        String finalPublisherExchange = publisherExchange;
                        wrappedQueueDefinition = new QueueDefinition()
                        {
                            @Override
                            public Class<? extends Annotation> annotationType()
                            {
                                return QueueDefinition.class;
                            }

                            @Override
                            public String value()
                            {
                                return envForName(queueName, "QUEUE_NAME", queueName);
                            }

                            @Override
                            public QueueOptions options()
                            {
                                return new QueueOptionsDefault();
                            }

                            @Override
                            public String exchange()
                            {
                                return envForName(queueName, "QUEUE_EXCHANGE", finalPublisherExchange);
                            }
                        };
                    }
                    else
                    {
                        wrappedQueueDefinition = wrapQueueDefinition(queueDefinition);
                    }
                }
                registerPublisherQueue(queueName, exchangeName, wrappedQueueDefinition, false);
                log.debug("Found Queue Publisher - {} - {} - {}", queueName, publisherExchange, queueRoutingKeys.get(queueName));
            }
        }
    }

    private QueueDefinition wrapQueueDefinition(QueueDefinition queueDefinition)
    {
        if (queueDefinition == null)
            return null;
        String rawQueueName = queueDefinition.value();
        return new QueueDefinition()
        {
            @Override
            public Class<? extends Annotation> annotationType()
            {
                return QueueDefinition.class;
            }

            @Override
            public String value()
            {
                return envForName(rawQueueName, "QUEUE_NAME", queueDefinition.value());
            }

            @Override
            public QueueOptions options()
            {
                return wrapQueueOptions(rawQueueName, queueDefinition.options());
            }

            @Override
            public String exchange()
            {
                return envForName(rawQueueName, "QUEUE_EXCHANGE", queueDefinition.exchange());
            }
        };
    }

    private QueueOptions wrapQueueOptions(String queueName, QueueOptions options)
    {
        if (options == null)
            return null;
        return new QueueOptions()
        {
            @Override
            public Class<? extends Annotation> annotationType()
            {
                return QueueOptions.class;
            }

            @Override
            public int priority()
            {
                return Integer.parseInt(envForName(queueName, "QUEUE_PRIORITY", String.valueOf(options.priority())));
            }

            @Override
            public int fetchCount()
            {
                return Integer.parseInt(envForName(queueName, "QUEUE_FETCH_COUNT", String.valueOf(options.fetchCount())));
            }

            @Override
            public boolean durable()
            {
                return Boolean.parseBoolean(envForName(queueName, "QUEUE_DURABLE", String.valueOf(options.durable())));
            }

            @Override
            public boolean delete()
            {
                return Boolean.parseBoolean(envForName(queueName, "QUEUE_DELETE", String.valueOf(options.delete())));
            }

            @Override
            public boolean autoAck()
            {
                return Boolean.parseBoolean(envForName(queueName, "QUEUE_AUTO_ACK", String.valueOf(options.autoAck())));
            }

            @Override
            public boolean consumerExclusive()
            {
                return Boolean.parseBoolean(envForName(queueName, "QUEUE_CONSUMER_EXCLUSIVE", String.valueOf(options.consumerExclusive())));
            }

            @Override
            public boolean singleConsumer()
            {
                return Boolean.parseBoolean(envForName(queueName, "QUEUE_SINGLE_CONSUMER", String.valueOf(options.singleConsumer())));
            }

            @Override
            public int ttl()
            {
                return Integer.parseInt(envForName(queueName, "QUEUE_TTL", String.valueOf(options.ttl())));
            }

            @Override
            public boolean noLocal()
            {
                return Boolean.parseBoolean(envForName(queueName, "QUEUE_NO_LOCAL", String.valueOf(options.noLocal())));
            }

            @Override
            public boolean keepMostRecent()
            {
                return Boolean.parseBoolean(envForName(queueName, "QUEUE_KEEP_MOST_RECENT", String.valueOf(options.keepMostRecent())));
            }

            @Override
            public int maxInternalQueueSize()
            {
                return Integer.parseInt(envForName(queueName, "QUEUE_MAX_INTERNAL_SIZE", String.valueOf(options.maxInternalQueueSize())));
            }

            @Override
            public boolean transacted()
            {
                return Boolean.parseBoolean(envForName(queueName, "QUEUE_TRANSACTED", String.valueOf(options.transacted())));
            }

            @Override
            public boolean autobind()
            {
                return Boolean.parseBoolean(envForName(queueName, "QUEUE_AUTOBIND", String.valueOf(options.autobind())));
            }

            @Override
            public int consumerCount()
            {
                return Integer.parseInt(envForName(queueName, "QUEUE_CONSUMER_COUNT", String.valueOf(options.consumerCount())));
            }

            @Override
            public boolean dedicatedChannel()
            {
                return Boolean.parseBoolean(envForName(queueName, "QUEUE_DEDICATED_CHANNEL", String.valueOf(options.dedicatedChannel())));
            }

            @Override
            public boolean dedicatedConnection()
            {
                return Boolean.parseBoolean(envForName(queueName, "QUEUE_DEDICATED_CONNECTION", String.valueOf(options.dedicatedConnection())));
            }
        };
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
        QueueDefinition wrapped = wrapQueueDefinition(queueDefinition);
        queueConsumerDefinitions.put(queueName, wrapped);
        queueConsumerClass.put(queueName, aClass);
        queueConsumerKeys.put(queueName, Key.get(aClass, Names.named(queueName)));

        String routingKey = exchangeName + "_" + wrapped.value();
        queueRoutingKeys.put(queueName, routingKey);

        log.debug("Found Queue Consumer - {} - {} - {}", queueName, exchangeName, routingKey);

        registerPublisherQueue(queueName, exchangeName, wrapped, true);
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
        QueueDefinition wrapped = wrapQueueDefinition(queueDefinition);
        if (!queuePublisherDefinitions.containsKey(queueName) || override)
        {
            queuePublisherDefinitions.put(queueName, wrapped);
            queuePublisherKeys.put(queueName, Key.get(QueuePublisher.class, Names.named(queueName)));
        }
        if (!queueRoutingKeys.containsKey(queueName) || override)
        {
            String routingKey = exchangeName + "_" + wrapped.value();
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

    /**
     * Resolves an environment variable or system property scoped by name.
     * <p>
     * Lookup order:
     * <ol>
     *   <li>{@code RABBITMQ_{NORMALIZED_NAME}_{PROPERTY}} — name-specific override</li>
     *   <li>{@code RABBITMQ_{PROPERTY}} — global fallback</li>
     *   <li>The supplied {@code defaultValue}</li>
     * </ol>
     * The name is normalized to uppercase with hyphens and dots replaced by underscores.
     *
     * @param name         The logical name (connection name, exchange name, or queue name).
     * @param property     The property suffix (e.g. {@code "HOST"}, {@code "DURABLE"}).
     * @param defaultValue The annotation default to use if no override is found.
     * @return The resolved value.
     */
    static String envForName(String name, String property, String defaultValue)
    {
        String normalizedName = name.toUpperCase().replace('-', '_').replace('.', '_');
        // Try name-specific first: RABBITMQ_{NAME}_{PROPERTY}
        String scopedKey = "RABBITMQ_" + normalizedName + "_" + property;
        String scopedValue = com.guicedee.client.Environment.getSystemPropertyOrEnvironment(scopedKey, null);
        if (scopedValue != null && !scopedValue.isBlank())
        {
            return scopedValue;
        }
        // Fall back to global: RABBITMQ_{PROPERTY}
        return com.guicedee.client.Environment.getSystemPropertyOrEnvironment("RABBITMQ_" + property, defaultValue);
    }

}
