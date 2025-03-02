import com.guicedee.guicedinjection.interfaces.*;
import com.guicedee.rabbit.implementations.*;

module com.guicedee.rabbit {
    uses com.guicedee.rabbit.implementations.def.OnQueueExchangeDeclared;
    exports com.guicedee.rabbit;
    exports com.guicedee.rabbit.implementations.def;

    requires transitive io.vertx.rabbitmq;
    requires com.guicedee.vertx;

    requires com.guicedee.client;
    requires static lombok;

    requires com.rabbitmq.client;
    requires jakarta.transaction;
    requires io.github.classgraph;
    requires org.apache.commons.lang3;

    provides IGuicePostStartup with RabbitPostStartup;
    provides IGuiceModule with RabbitMQModule;
    provides com.guicedee.guicedinjection.interfaces.IGuicePreStartup with RabbitMQPreStartup;

    opens com.guicedee.rabbit.support to com.google.guice;
    opens com.guicedee.rabbit to com.google.guice,com.fasterxml.jackson.databind;
    opens com.guicedee.rabbit.implementations.def to com.google.guice,com.fasterxml.jackson.databind;
    exports com.guicedee.rabbit.implementations;
    opens com.guicedee.rabbit.implementations to com.fasterxml.jackson.databind, com.google.guice;
}