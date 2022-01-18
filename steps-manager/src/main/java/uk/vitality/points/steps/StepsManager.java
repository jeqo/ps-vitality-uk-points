package uk.vitality.points.steps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import uk.vitality.points.steps.model.*;
import uk.vitality.points.steps.serde.JsonSerde;

import java.time.Duration;
import java.util.function.Supplier;
import java.util.stream.Collectors;

class StepsManager implements Supplier<Topology> {

    @Override
    public Topology get() {
        var b = new StreamsBuilder();
        // only persons
        var personsTable = b.stream("entities", Consumed.with(Serdes.String(), JsonSerde.withType(Entity.class)).withName("read-entity-events"))
                .filter((id, e) -> "person".equals(e.entityType()))
                .toTable(Materialized.with(Serdes.String(), JsonSerde.withType(Entity.class)));
        // partitioned by personId
        var policiesTable = b.stream("policies", Consumed.with(Serdes.String(), JsonSerde.withType(Policy.class)))
                .flatMap((id, policy) -> policy.people().stream().map(p -> KeyValue.pair(p, policy)).collect(Collectors.toList()))
                .toTable(Materialized.with(Serdes.String(), JsonSerde.withType(Policy.class)));
        // join persons and policies
        var customerTable = personsTable.join(policiesTable, StepsManager::entityPolicyJoin);

//        b.globalTable("steps-points",
//                Consumed.with(Serdes.String(), JsonSerde.withType(StepsPoints.class)),
//                Materialized.with(Serdes.String(), JsonSerde.withType(StepsPoints.class)));

        b.stream("steps", Consumed.with(Serdes.String(), JsonSerde.withType(Steps.class)).withName("read-steps-events"))
                .join(customerTable, StepsManager::stepsCustomerJoin)
                .groupByKey(Grouped.as("group-customer-steps-by-entity-id"))
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofDays(1), Duration.ofDays(30)))
                .reduce(StepsManager::stepsSum, Named.as("sum-steps"), Materialized.with(Serdes.String(), JsonSerde.withType(CustomerSteps.class)))
                .toStream(Named.as("to-changelog"))
//                .transformValues(StepsManager::addPoints, "steps-points")
                .map(StepsManager::stepsToPoints, Named.as("map-steps-to-points"))
                // partition by policy
                // apply limits per policy
                // partition by person
                // apply limits per person
                .to("activity-points", Produced.with(Serdes.String(), JsonSerde.withType(ActivityPoints.class)).withName("write-points-from-steps"));
        return b.build();

        // point limits: (per person)
        // - 40 per week
        // - 8 per day

        // queries:
        // - per year:
        //   - per person
        //   - per policy (dependents)
    }

    private static KeyValue<String, ActivityPoints> stepsToPoints(Windowed<String> windowed, CustomerSteps customerSteps) {
        System.out.println("Mapping to points: " + windowed.key() + " and steps: " + customerSteps);
        return KeyValue.pair(windowed.key(),
                new ActivityPoints(customerSteps.customer().policy(),
                        customerSteps.customer().entity(),
                        "steps",
                        customerSteps.dateOfRecording(),
                        1, // to update via lookup
                        "some steps" // to update via range
                ));
    }

    static CustomerSteps stepsSum(CustomerSteps left, CustomerSteps right) {
        System.out.println("Sum " + left.stepsCount() + " and " + right.stepsCount());
        return new CustomerSteps(left.customer(), left.dateOfRecording(), left.stepsCount() + right.stepsCount());
    }

    static ValueTransformerWithKey<Windowed<Object>, Object, Object> addPoints() {
        throw new UnsupportedOperationException("yet");
    }

    static CustomerSteps stepsCustomerJoin(String entityId, Steps steps, Customer customer) {
        return new CustomerSteps(customer, steps.dateOfRecording().toLocalDate(), steps.stepsCount());
    }

    static Customer entityPolicyJoin(Entity entity, Policy policy) {
        return new Customer(entity, policy);
    }
}
