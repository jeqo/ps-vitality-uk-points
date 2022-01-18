package uk.vitality.points.steps.model;

import java.time.LocalDateTime;

public record Steps (
        String entityId,
        LocalDateTime dateOfRecording,
        Integer stepsCount
) {
}
