package com.guicedee.rabbit;

import com.guicedee.rabbit.implementations.RabbitMQModule;
import com.guicedee.rabbit.implementations.def.RabbitMQClientProvider;
import io.vertx.rabbitmq.RabbitMQClient;

import java.util.Map;
import java.util.Optional;

public class RabbitMQUtils {

    public static Optional<RabbitMQClient> getClientForPackage(String packageName) {
        for (Map.Entry<String, RabbitMQClientProvider> entry : RabbitMQModule.packageClients.entrySet()) {
            String key = entry.getKey();
            RabbitMQClientProvider client = entry.getValue();
            if (packageName.startsWith(key)) {
                return Optional.ofNullable(client.get());
            }
        }
        return Optional.empty();
    }

}
