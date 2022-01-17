package uk.vitality.points.steps.model;

public record ActivityPoints(
        Policy policy,
        Entity customer,
        Integer year,
        Integer points
) {
}
