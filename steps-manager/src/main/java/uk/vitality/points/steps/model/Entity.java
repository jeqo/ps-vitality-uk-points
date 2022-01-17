package uk.vitality.points.steps.model;

import java.time.LocalDate;

public record Entity(
        String entityId,
        String gender,
        LocalDate dateOfBirth
) {
}
