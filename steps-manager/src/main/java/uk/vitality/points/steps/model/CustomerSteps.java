package uk.vitality.points.steps.model;

import java.time.LocalDate;

public record CustomerSteps(
        Policy policy,
        Entity customer,
        LocalDate dateOfRecording,
        Integer stepsCount
) {
}
