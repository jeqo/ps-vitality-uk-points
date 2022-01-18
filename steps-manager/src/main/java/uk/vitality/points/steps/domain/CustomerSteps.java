package uk.vitality.points.steps.domain;

import java.time.LocalDate;

public record CustomerSteps(
        Customer customer,
        LocalDate dateOfRecording,
        Integer stepsCount
) {
}
