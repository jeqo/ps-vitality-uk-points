package uk.vitality.points.steps.model;

import java.time.LocalDate;

public record Steps (
        String entityId,
        LocalDate dateOfRecording,
        Integer stepsCount
) {
}
