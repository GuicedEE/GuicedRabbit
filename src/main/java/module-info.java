import com.guicedee.guicedinjection.interfaces.IGuiceModule;
import com.guicedee.guicedinjection.interfaces.IGuicePostStartup;
import com.guicedee.rabbit.implementations.RabbitMQModule;
import com.guicedee.rabbit.implementations.RabbitPostStartup;

module com.guicedee.rabbit {
    requires transitive io.vertx.rabbitmq;

    requires com.guicedee.client;
    requires static lombok;

    requires com.rabbitmq.client;

    provides IGuicePostStartup with RabbitPostStartup;
    provides IGuiceModule with RabbitMQModule;

    opens com.guicedee.rabbit.implementations to com.google.guice;
}