package uk.vitality.points.steps;

import kafka.streams.rest.armeria.HttpKafkaStreamsServer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Server {
    public static void main(String[] args) throws IOException {
        final var streamsProps = loadProperties(args);

        final var stepsManager = new StepsManager();
        final var topology = stepsManager.get();

        System.out.println("Kafka Streams topology: \n" + topology.describe().toString());

        HttpKafkaStreamsServer.newBuilder()
                .port(8080)
                .build(topology, streamsProps)
                .startApplicationAndServer();
    }

    private static Properties loadProperties(String[] args) throws IOException {
        var name = "steps-manager/src/main/resources/streams";
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
