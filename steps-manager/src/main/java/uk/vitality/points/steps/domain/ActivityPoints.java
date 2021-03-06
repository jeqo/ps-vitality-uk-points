package uk.vitality.points.steps.domain;

import java.time.LocalDate;

public record ActivityPoints(
        Policy policy,
        Entity customer,
        String activityType,
        LocalDate date,
        Integer steps,
        Integer points,
        String reason
) {
}
