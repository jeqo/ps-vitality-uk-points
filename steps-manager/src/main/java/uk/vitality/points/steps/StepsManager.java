package uk.vitality.points.steps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import uk.vitality.points.steps.model.*;
import uk.vitality.points.steps.serde.JsonSerde;

import java.time.Duration;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

class StepsManager implements Supplier<Topology> {

    @Override
    public Topology get() {
        var b = new StreamsBuilder();
        // only persons
        var personsTable = b.stream("entities",
                        Consumed.with(Serdes.String(), JsonSerde.withType(Entity.class)).withName("read-entity-events"))
                .filter((id, e) -> "person".equals(e.entityType()))
                .toTable(Materialized.with(Serdes.String(), JsonSerde.withType(Entity.class)));
        // partitioned by personId
        var policiesTable = b.stream("policies",
                        Consumed.with(Serdes.String(), JsonSerde.withType(Policy.class)))
                .flatMap((id, policy) -> policy.people().stream().map(p -> KeyValue.pair(p, policy)).collect(Collectors.toList()))
                .toTable(Materialized.with(Serdes.String(), JsonSerde.withType(Policy.class)));
        // join persons and policies
        var customerTable = personsTable.join(policiesTable, StepsManager::entityPolicyJoin);

        // enrich with customers metadata
        final var enrichedStepsStream = b
                .stream("steps", Consumed.with(Serdes.String(), JsonSerde.withType(Steps.class)).withName("read-steps-events"))
                .join(customerTable, StepsManager::stepsCustomerJoin);

        // windowing to sum steps
        final var sumStepsStream = enrichedStepsStream
                .groupByKey(Grouped.as("group-customer-steps-by-entity-id"))
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofDays(1), Duration.ofDays(30)))
                .reduce(StepsManager::stepsSum, Named.as("sum-steps"), Materialized.with(Serdes.String(), JsonSerde.withType(CustomerSteps.class)))
                .toStream(Named.as("to-changelog"));

        // rules to map steps to points
        b.globalTable("steps-points",
                Consumed.with(Serdes.String(), JsonSerde.withType(StepsPoints.class)),
                Materialized.as("steps-points"));

        // map steps to points
        final var pointsStream = sumStepsStream
                .transform(StepsManager::addPoints);

        // validate points per day
        b.addStateStore(Stores.windowStoreBuilder(
                Stores.persistentWindowStore(
                        "points-per-day",
                        Duration.ofDays(7), // retention
                        Duration.ofDays(1), // window size
                        false),
                Serdes.String(),
                JsonSerde.withType(ActivityPoints.class)));

        final var pointsValidPerDay = pointsStream
                .peek((key, value) -> System.out.println("Points before checking: " + value))
                .transformValues(StepsManager::validateMaxPointsPerDay, "points-per-day")
                .peek((key, value) -> System.out.println("Points after checking: " + value))
                .filterNot((key, value) -> Objects.isNull(value));

        // downstream activity points
        pointsValidPerDay.to("activity-points",
                Produced.with(Serdes.String(), JsonSerde.withType(ActivityPoints.class))
                        .withName("write-points-from-steps"));
        return b.build();

        // point limits: (per person)
        // - 40 per week
        // - 8 per day

        // queries:
        // - per year:
        //   - per person
        //   - per policy (dependents)
    }

    static CustomerSteps stepsSum(CustomerSteps left, CustomerSteps right) {
        System.out.println("Sum " + left.stepsCount() + " and " + right.stepsCount());
        return new CustomerSteps(left.customer(), left.dateOfRecording(), left.stepsCount() + right.stepsCount());
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
                if (points == null) {
                    if (value.points() > 0) {
                        pointsPerDay.put(key, points, of);
                        return value;
                    } else return null;
                } else {
                    if (points == 8) {
                        return null;
                    } else {
                        if (value.points() > 0) {
                            pointsPerDay.put(key, points, of);
                            return value;
                        } else return null;
                    }
                }
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
