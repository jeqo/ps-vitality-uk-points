package uk.vitality.points.steps.model;

import java.time.LocalDate;
import java.util.List;

public record Policy(
        String policyId,
        LocalDate effectiveFrom,
        LocalDate effectiveTo,
        String principal,
        List<String> people
) {
}
