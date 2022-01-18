package uk.vitality.points.steps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import uk.vitality.points.steps.model.*;
import uk.vitality.points.steps.serde.JsonSerde;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

class StepsManager implements Supplier<Topology> {

    @Override
    public Topology get() {
        var builder = new StreamsBuilder();
        // 1. Prepare Metadata to enrich Steps
        // only persons
        var personsTable = builder.stream("entities",
                        Consumed.with(Serdes.String(), JsonSerde.withType(Entity.class))
                                .withName("read-entity-events"))
                .filter((id, e) -> "person".equals(e.entityType()), Named.as("only-persons"))
                .toTable(Named.as("to-only-persons-table"), Materialized.<String, Entity, KeyValueStore<Bytes, byte[]>>as("persons")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(JsonSerde.withType(Entity.class)));
        // partitioned by personId
        var policiesTable = builder.stream("policies",
                        Consumed.with(Serdes.String(), JsonSerde.withType(Policy.class))
                                .withName("read-policies"))
                .flatMap((id, policy) -> policy.people().stream().map(p -> KeyValue.pair(p, policy)).collect(Collectors.toList()),
                        Named.as("to-policy-per-person"))
                .toTable(Named.as("to-policy-person-table"), Materialized.<String, Policy, KeyValueStore<Bytes, byte[]>>as("policies")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(JsonSerde.withType(Policy.class)));
        // join persons and policies
        var customerTable = personsTable.join(policiesTable, StepsManager::entityPolicyJoin,
                Named.as("join-entity-and-policy"),
                Materialized.<String, Customer, KeyValueStore<Bytes, byte[]>>as("customers")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(JsonSerde.withType(Customer.class)));

        // 2. Enrich Steps with customer metadata
        final var enrichedStepsStream = builder
                .stream("steps", Consumed.with(Serdes.String(), JsonSerde.withType(Steps.class))
                        .withTimestampExtractor((record, partitionTime) -> {
                            var r = (Steps) record.value();
                            return r.dateOfRecording().toInstant(ZoneOffset.UTC).toEpochMilli();
                        })
                        .withName("read-steps-events"))
//                .transformValues(StepsManager::setStepsTimestamp, Named.as("set-steps-timestamp"))
                .join(customerTable, StepsManager::stepsCustomerJoin, Joined.as("join-steps-and-customer"));

        // 3. Sum steps per day
        final var sumStepsStream = enrichedStepsStream
                .groupByKey(Grouped.as("group-customer-steps-by-entity-id"))
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofDays(1), Duration.ofDays(30)))
                .reduce(StepsManager::stepsSum, Named.as("sum-steps"),
                        Materialized.<String, CustomerSteps, WindowStore<Bytes, byte[]>>as("steps-sum")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(JsonSerde.withType(CustomerSteps.class)))
                .toStream(Named.as("to-changelog"));

        // 4. Map Steps to Points
        // rules to map steps to points
        builder.globalTable("steps-points",
                Consumed.with(Serdes.String(), JsonSerde.withType(StepsPoints.class))
                        .withName("read-steps-points-rules"),
                Materialized.<String, StepsPoints, KeyValueStore<Bytes, byte[]>>as("steps-points")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(JsonSerde.withType(StepsPoints.class)));

        // map steps to points
        final var pointsStream = sumStepsStream
                .transform(StepsManager::addPoints, Named.as("add-points"));

        // 5. Validate points per day
        // Prepare window store per day
        builder.addStateStore(Stores.windowStoreBuilder(
                Stores.persistentWindowStore(
                        "points-per-day",
                        Duration.ofDays(7), // retention
                        Duration.ofDays(1), // window size
                        false),
                Serdes.String(),
                JsonSerde.withType(ActivityPoints.class)));

        // Validate points
        final var pointsValidPerDay = pointsStream
                .peek((key, value) -> System.out.println("Points before checking: " + value), Named.as("peek-points-before"))
                .transformValues(StepsManager::validateMaxPointsPerDay, Named.as("validate-points-per-day"), "points-per-day")
                .peek((key, value) -> System.out.println("Points after checking: " + value), Named.as("peek-points-after"))
                .filterNot((key, value) -> Objects.isNull(value), Named.as("filter-only-when-points-change"));

        // 6. Write activity points
        // downstream activity points
        pointsValidPerDay.to("activity-points",
                Produced.with(Serdes.String(), JsonSerde.withType(ActivityPoints.class))
                        .withName("write-points-from-steps"));
        return builder.build();

        // point limits: (per person)
        // - 40 per week
        // - 8 per day

        // queries:
        // - per year:
        //   - per person
        //   - per policy (dependents)
    }

    static CustomerSteps stepsSum(CustomerSteps left, CustomerSteps right) {
        System.out.println("Sum steps from customer " + left.customer() + ": " + left.stepsCount() + " and " + right.stepsCount());
        return new CustomerSteps(left.customer(), left.dateOfRecording(), left.stepsCount() + right.stepsCount());
    }

    static  ValueTransformerWithKey<String, Steps, Steps> setStepsTimestamp() {
        return new ValueTransformerWithKey<>() {
            ProcessorContext context;

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
            }

            @Override
            public Steps transform(String readOnlyKey, Steps value) {
                context.forward(readOnlyKey, value,
                        To.all().withTimestamp(
                                value.dateOfRecording()
                                        .toInstant(ZoneOffset.UTC)
                                        .toEpochMilli()));
                return null;
            }

            @Override
            public void close() {
            }
        };
    }

    static ValueTransformer<ActivityPoints, ActivityPoints> validateMaxPointsPerDay() {
        return new ValueTransformer<>() {
            WindowStore<String, Integer> pointsPerDay;

            @Override
            public void init(ProcessorContext context) {
                pointsPerDay = context.getStateStore("points-per-day");
            }

            @Override
            public ActivityPoints transform(ActivityPoints value) {
                var key = value.policy().policyId() + "-"
                          + value.customer().entityId() + "-"
                          + value.date().format(DateTimeFormatter.BASIC_ISO_DATE);
                final var of = ZonedDateTime.of(value.date(), LocalTime.of(12, 0), ZoneId.systemDefault())
                        .toInstant().toEpochMilli();
                final var points = pointsPerDay.fetch(key, of);
                System.out.println("Prev points: " + points);
                if (points == null) { // if no previous points
                    if (value.points() == 0) return null; // if points to emit are higher than 0
                } else { // if previous points
                    // if already emitted max per day
                    if (points == 8) return null;
                    // if points to emit are higher than 0
                    else if (value.points() == 0) return null;
                }
                pointsPerDay.put(key, points, of);
                return value;
            }

            @Override
            public void close() {
                // Nothing to close
            }
        };
    }

    static Transformer<Windowed<String>, CustomerSteps, KeyValue<String, ActivityPoints>> addPoints() {
        return new Transformer<>() {
            KeyValueStore<String, ValueAndTimestamp<StepsPoints>> stepsPointsStore;

            @Override
            public void init(ProcessorContext context) {
                stepsPointsStore = context.getStateStore("steps-points");
            }

            @Override
            public KeyValue<String, ActivityPoints> transform(Windowed<String> readOnlyKey, CustomerSteps customerSteps) {
                int points = -1;
                int from = -1;
                try (var iter = stepsPointsStore.all()) {
                    while (iter.hasNext()) {
                        final var kv = iter.next();
                        final var value = kv.value.value();
                        if (points < 0) {
                            if (customerSteps.stepsCount() > value.stepsFrom()
                                && customerSteps.stepsCount() <= value.stepsTo()) {
                                from = value.stepsFrom();
                                points = value.points();
                            }
                        }
                    }
                }
                return KeyValue.pair(readOnlyKey.key(),
                        new ActivityPoints(customerSteps.customer().policy(),
                                customerSteps.customer().entity(),
                                "steps",
                                customerSteps.dateOfRecording(),
                                customerSteps.stepsCount(),
                                points,
                                "+" + from + " steps"
                        ));
            }

            @Override
            public void close() {
                // nothing to close
            }
        };
    }

    static CustomerSteps stepsCustomerJoin(String entityId, Steps steps, Customer customer) {
        return new CustomerSteps(customer, steps.dateOfRecording().toLocalDate(), steps.stepsCount());
    }

    static Customer entityPolicyJoin(Entity entity, Policy policy) {
        return new Customer(entity, policy);
    }
}
