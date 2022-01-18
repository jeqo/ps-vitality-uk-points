package uk.vitality.points.steps.model;

import java.time.LocalDate;

public record CustomerSteps(
        Customer customer,
        LocalDate dateOfRecording,
        Integer stepsCount
) {
}
