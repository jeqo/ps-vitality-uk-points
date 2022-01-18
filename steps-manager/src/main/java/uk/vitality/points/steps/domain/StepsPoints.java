package uk.vitality.points.steps.domain;

// + 12.5K per day -> 8
// > 10 K per day -> 5
// > 7 K per day -> 3

// if 18 age < no steps
// if 65 >
public record StepsPoints(
        Integer stepsFrom,
        Integer stepsTo,
        Integer points
) {
}
