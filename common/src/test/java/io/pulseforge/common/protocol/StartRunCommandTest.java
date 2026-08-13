package io.pulseforge.common.protocol;

import static org.assertj.core.api.Assertions.assertThat;

import io.pulseforge.common.domain.HttpMethod;
import io.pulseforge.common.domain.LoadProfile;
import io.pulseforge.common.domain.Scenario;
import io.pulseforge.common.domain.ScenarioStep;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Rate sharding has to be exact. If the shards do not sum to the requested rate, every run silently
 * measures a different load than the one the scenario asked for.
 */
class StartRunCommandTest {

    @ParameterizedTest
    @CsvSource({"400, 5", "200, 5", "100, 3", "7, 5", "1000, 7", "13, 4"})
    @DisplayName("shard rates always sum back to the requested arrival rate")
    void shardsSumToTheRequestedRate(int arrivalRate, int workerCount) {
        StartRunCommand command = command(arrivalRate, workerCount);

        double total = 0;
        for (int shard = 0; shard < workerCount; shard++) {
            total += command.rateForShard(shard);
        }

        assertThat(total)
                .as("integer division would quietly lose up to workerCount-1 requests per second")
                .isEqualTo(arrivalRate);
    }

    @Test
    @DisplayName("the remainder goes to the lowest shards, one request each")
    void remainderIsDistributedOnePerShard() {
        // 13 across 4 workers: 3 each, remainder 1 -> shards 0 gets 4, the rest get 3.
        StartRunCommand command = command(13, 4);

        assertThat(command.rateForShard(0)).isEqualTo(4);
        assertThat(command.rateForShard(1)).isEqualTo(3);
        assertThat(command.rateForShard(2)).isEqualTo(3);
        assertThat(command.rateForShard(3)).isEqualTo(3);
    }

    @Test
    @DisplayName("no shard is ever starved when there are more workers than requests per second")
    void handlesMoreWorkersThanRate() {
        StartRunCommand command = command(3, 5);

        assertThat(command.rateForShard(0)).isEqualTo(1);
        assertThat(command.rateForShard(4)).isZero();

        double total = 0;
        for (int shard = 0; shard < 5; shard++) {
            total += command.rateForShard(shard);
        }
        assertThat(total).isEqualTo(3);
    }

    private static StartRunCommand command(int arrivalRate, int workerCount) {
        Scenario scenario =
                new Scenario(
                        "test",
                        "http://target:8080",
                        new LoadProfile(Duration.ofSeconds(10), Duration.ZERO, arrivalRate),
                        List.of(new ScenarioStep(null, HttpMethod.GET, "/api/fast", 1, null, null)),
                        List.of());
        return new StartRunCommand(UUID.randomUUID(), scenario, Instant.now(), workerCount);
    }
}
