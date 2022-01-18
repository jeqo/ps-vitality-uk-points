package uk.vitality.points.steps.domain;

import java.time.LocalDateTime;

public record Steps (
        String entityId,
        LocalDateTime dateOfRecording,
        Integer stepsCount
) {
}
