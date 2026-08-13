package io.pulseforge.worker.run;

import io.pulseforge.common.domain.Scenario;
import io.pulseforge.common.domain.ScenarioStep;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Picks which step a scheduled request should execute, honouring the declared weights.
 *
 * <p>Selection is random rather than round-robin so that concurrent shards do not fall into
 * lockstep and produce a synthetic request pattern the target would never see in production.
 * The cumulative weight table is built once; selection is a binary search.
 */
public class StepSelector {

    private final List<ScenarioStep> steps;
    private final int[] cumulativeWeights;
    private final int totalWeight;

    public StepSelector(Scenario scenario) {
        this.steps = scenario.steps();
        this.cumulativeWeights = new int[steps.size()];
        int running = 0;
        for (int i = 0; i < steps.size(); i++) {
            running += steps.get(i).weight();
            cumulativeWeights[i] = running;
        }
        this.totalWeight = running;
    }

    /** Index into the scenario's step list. Returned as an index so metrics can key off it. */
    public int nextIndex() {
        if (steps.size() == 1) {
            return 0;
        }
        int roll = ThreadLocalRandom.current().nextInt(totalWeight);
        int low = 0;
        int high = cumulativeWeights.length - 1;
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (roll < cumulativeWeights[mid]) {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        return low;
    }

    public ScenarioStep step(int index) {
        return steps.get(index);
    }

    public List<ScenarioStep> steps() {
        return steps;
    }
}
