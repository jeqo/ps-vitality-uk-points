package uk.vitality.points.steps;

import org.apache.kafka.streams.KafkaStreams;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Main {
    public static void main(String[] args) throws IOException {
        final var streamsProps = loadProperties(args);

        final var stepsManager = new StepsManager();
        final var topology = stepsManager.get();

        final var ks = new KafkaStreams(topology, streamsProps);
        Runtime.getRuntime().addShutdownHook(new Thread(ks::close));
        ks.start();
    }

    private static Properties loadProperties(String[] args) throws IOException {
        var name = "src/main/resources/streams";
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
