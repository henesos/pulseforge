package io.pulseforge.controlplane.api;

import io.pulseforge.controlplane.results.AssertionEvaluator;
import io.pulseforge.controlplane.results.RunResults;

/**
 * The full report for a run: what was measured, whether it passed, and whether the measurement
 * itself can be trusted.
 *
 * @param verdict     PASS/FAIL from the scenario's assertions; null when the scenario declared none
 * @param complete    false when samples were dropped or requests skipped — the numbers above are
 *                    then computed from an incomplete population and should be read with that in
 *                    mind
 */
public record RunResultsResponse(
        RunResults results, AssertionEvaluator.Verdict verdict, boolean complete) {}
