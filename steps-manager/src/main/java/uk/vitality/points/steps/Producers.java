package uk.vitality.points.steps;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import uk.vitality.points.steps.domain.Entity;
import uk.vitality.points.steps.domain.Policy;
import uk.vitality.points.steps.domain.Steps;
import uk.vitality.points.steps.domain.StepsPoints;
import uk.vitality.points.steps.serde.JsonSerde;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;

public class Producers {
    static class StepsPointsProducer {
        // + 12.5K per day -> 8
        // > 10 K per day -> 5
        // > 7 K per day -> 3
        public static void main(String[] args) throws IOException {
            final var props = loadProperties(args);
            final var producer = new KafkaProducer<>(props, new StringSerializer(), new JsonSerde<>(StepsPoints.class).serializer());
            final var stepsPoints0 = new StepsPoints(0, 7_000, 0);
            producer.send(new ProducerRecord<>("steps-points", "rule_0", stepsPoints0));
            final var stepsPoints1 = new StepsPoints(7_000, 10_000, 3);
            producer.send(new ProducerRecord<>("steps-points", "rule_1", stepsPoints1));
            final var stepsPoints2 = new StepsPoints(10_000, 12_500, 5);
            producer.send(new ProducerRecord<>("steps-points", "rule_2", stepsPoints2));
            final var stepsPoints3 = new StepsPoints(12_500, Integer.MAX_VALUE, 8);
            producer.send(new ProducerRecord<>("steps-points", "rule_3", stepsPoints3));
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
                    List.of("customer_1", "customer_2", "customer_3", "customer_4")
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
            final var entity1 = new Entity("customer_1", "person",
                    "F", LocalDate.of(1990, 10, 13));
            producer.send(new ProducerRecord<>("entities", entity1.entityId(), entity1));
            final var entity2 = new Entity("customer_2", "person",
                    "F", LocalDate.of(1989, 12, 12));
            producer.send(new ProducerRecord<>("entities", entity2.entityId(), entity2));
            final var entity3 = new Entity("customer_3", "person",
                    "M", LocalDate.of(1991, 1, 3));
            producer.send(new ProducerRecord<>("entities", entity3.entityId(), entity3));
            final var entity4 = new Entity("customer_4", "person",
                    "M", LocalDate.of(1992, 11, 1));
            producer.send(new ProducerRecord<>("entities", entity4.entityId(), entity4));
            producer.close();
        }
    }


    static class StepsProducer {
        public static void main(String[] args) throws IOException {
            final var props = loadProperties(args);
            final var producer = new KafkaProducer<>(props, new StringSerializer(), new JsonSerde<>(Steps.class).serializer());
            final var steps = new Steps("customer_2",
                    LocalDateTime.of(2022, 1, 19, 20, 0, 0),
                    11000);
            producer.send(new ProducerRecord<>("steps", steps.entityId(), steps));
            producer.close();
        }
    }

    private static Properties loadProperties(String[] args) throws IOException {
        var name = "steps-manager/src/main/resources/producer";
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
