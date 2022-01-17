package uk.vitality.points.steps;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import uk.vitality.points.steps.model.Entity;
import uk.vitality.points.steps.model.Policy;
import uk.vitality.points.steps.model.Steps;
import uk.vitality.points.steps.serde.JsonSerde;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.util.List;
import java.util.Properties;

public class Producers {
    static class StepsProducer {
        public static void main(String[] args) throws IOException {
            final var props = loadProperties(args);
            final var producer = new KafkaProducer<>(props, new StringSerializer(), new JsonSerde<>(Steps.class).serializer());
            final var steps = new Steps("entity_1", LocalDate.of(2022, 1, 17), 1000);
            producer.send(new ProducerRecord<>("steps", steps.entityId(), steps));
            producer.close();
        }
    }

    static class PolicyProducer {
        public static void main(String[] args) throws IOException {
            final var props = loadProperties(args);
            final var producer = new KafkaProducer<>(props,
                    new StringSerializer(),
                    new JsonSerde<>(Policy.class).serializer());
            final var policy = new Policy("policy_1",
                    LocalDate.of(2021, 1, 1),
                    LocalDate.of(2025, 12, 31),
                    "customer_1",
                    List.of("customer_1", "customer_2")
            );
            producer.send(new ProducerRecord<>("policies", policy.policyId(), policy));
            producer.close();
        }
    }

    static class EntityProducer {
        public static void main(String[] args) throws IOException {
            final var props = loadProperties(args);
            final var producer = new KafkaProducer<>(props,
                    new StringSerializer(),
                    new JsonSerde<>(Entity.class).serializer());
            final var entity = new Entity("entity_1",
                    "F", LocalDate.of(1990, 10, 10));
            producer.send(new ProducerRecord<>("entities", entity.entityId(), entity));
            producer.close();
        }
    }


    private static Properties loadProperties(String[] args) throws IOException {
        var name = "src/main/resources/producer";
        final var streamsProps = new Properties();
        if (args.length >= 1) {
            name = name + "_" + args[0];
        }
        final var configFile = name + ".properties";
        try (final var inputStream = new FileInputStream(configFile)) {
            System.out.printf("Loading config file %s%n", configFile);
            streamsProps.load(inputStream);
        }
        return streamsProps;
    }
}
