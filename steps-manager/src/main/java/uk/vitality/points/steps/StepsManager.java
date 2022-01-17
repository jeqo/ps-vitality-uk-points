package uk.vitality.points.steps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.function.Supplier;

class StepsManager implements Supplier<Topology> {

    @Override
    public Topology get() {
        var b = new StreamsBuilder();
//        var entitiesTable = b.stream("entities").filter((k, v) -> true).toTable();
//        var policiesTable = b.table("policies");
//        b.globalTable("steps-points");

//        var customerTable = entitiesTable.join(policiesTable, StepsManager::entityPolicyJoin);

        b.stream("steps", Consumed.with(Serdes.String(), Serdes.Integer()).withName("read-steps-events"))
//                .join(customerTable, StepsManager::stepsCustomerJoin)
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofDays(1), Duration.ofDays(7)))
                .reduce(StepsManager::stepsSum, Named.as("sum-steps"))
                .toStream(Named.as("to-changelog"))
//                .transformValues(StepsManager::addPoints, "steps-points")
                .map(StepsManager::stepsToPoints, Named.as("map-steps-to-points"))
                .to("activity-points", Produced.with(Serdes.String(), Serdes.String()).withName("write-points-from-steps"));
        return b.build();
    }

    private static KeyValue<String, String> stepsToPoints(Windowed<String> windowed, Integer count) {
        System.out.println("Mapping to points: " + windowed.key() + " and count: " + count );
        return KeyValue.pair(windowed.key(), String.valueOf(count));
    }

    static Integer stepsSum(Integer left, Integer right) {
        System.out.println("Sum " + left + " and " + right);
        return left + right;
    }

    static ValueTransformerWithKey<Windowed<Object>, Object, Object> addPoints() {
        throw new UnsupportedOperationException("yet");
    }

    static Object stepsCustomerJoin(Object o, Object o1, Object o2) {
        throw new UnsupportedOperationException("yet");
    }

    static <VR, VO> VR entityPolicyJoin(Object o, VO vo) {
        throw new UnsupportedOperationException("yet");
    }
}
