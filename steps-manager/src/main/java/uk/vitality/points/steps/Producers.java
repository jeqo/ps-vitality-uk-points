package uk.vitality.points.steps;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Producers {
    static class StepsProducer {
        public static void main(String[] args) throws IOException {
            final var props = loadProperties(args);
            final var producer = new KafkaProducer<>(props, new StringSerializer(), new IntegerSerializer());
            producer.send(new ProducerRecord<>("steps", "customer-4", 10));
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
