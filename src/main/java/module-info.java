import com.guicedee.guicedinjection.interfaces.IGuiceModule;
import com.guicedee.guicedinjection.interfaces.IGuicePostStartup;
import com.guicedee.rabbit.implementations.RabbitMQModule;
import com.guicedee.rabbit.implementations.RabbitPostStartup;

module com.guicedee.rabbit {
    uses com.guicedee.rabbit.implementations.def.OnQueueExchangeDeclared;
    exports com.guicedee.rabbit;
    exports com.guicedee.rabbit.implementations.def;

    requires transitive io.vertx.rabbitmq;
    requires guiced.vertx;

    requires com.guicedee.client;
    requires static lombok;

    requires com.rabbitmq.client;
    requires jakarta.transaction;
    requires io.github.classgraph;

    provides IGuicePostStartup with RabbitPostStartup;
    provides IGuiceModule with RabbitMQModule;

    opens com.guicedee.rabbit.implementations to com.google.guice;
    opens com.guicedee.rabbit.support to com.google.guice;
    opens com.guicedee.rabbit to com.google.guice,com.fasterxml.jackson.databind;
    opens com.guicedee.rabbit.implementations.def to com.google.guice,com.fasterxml.jackson.databind;
}