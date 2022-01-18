package uk.vitality.points.steps.domain;

import java.time.LocalDate;

public record Entity(
        String entityId,
        String entityType,
        String gender,
        LocalDate dateOfBirth
) {
}
