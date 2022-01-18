package uk.vitality.points.steps.model;

import java.time.LocalDate;

public record ActivityPoints(
        Policy policy,
        Entity customer,
        String activityType,
        LocalDate date,
        Integer points,
        String reason
) {
}
