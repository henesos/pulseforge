package io.pulseforge.common.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.pulseforge.common.domain.AssertionType;
import io.pulseforge.common.domain.ComparisonOperator;
import io.pulseforge.common.domain.HttpMethod;
import io.pulseforge.common.domain.Scenario;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ScenarioParserTest {

    private static final String VALID =
            """
            name: checkout-flow-baseline
            target: http://target-service:8081
            duration: 120s
            rampUp: 30s
            arrivalRate: 400
            steps:
              - method: GET
                path: /api/fast
                weight: 70
              - method: POST
                path: /api/slow
                weight: 30
                body: '{"item":"sku-1"}'
            assertions:
              - p95 < 250ms
              - errorRate < 1%
            """;

    @Test
    @DisplayName("parses the reference scenario into domain values")
    void parsesReferenceScenario() {
        Scenario scenario = ScenarioParser.parse(VALID);

        assertThat(scenario.name()).isEqualTo("checkout-flow-baseline");
        assertThat(scenario.target()).isEqualTo("http://target-service:8081");
        assertThat(scenario.load().duration()).isEqualTo(Duration.ofSeconds(120));
        assertThat(scenario.load().rampUp()).isEqualTo(Duration.ofSeconds(30));
        assertThat(scenario.load().arrivalRate()).isEqualTo(400);

        assertThat(scenario.steps()).hasSize(2);
        assertThat(scenario.steps().get(0).method()).isEqualTo(HttpMethod.GET);
        assertThat(scenario.steps().get(0).weight()).isEqualTo(70);
        assertThat(scenario.steps().get(1).body()).isEqualTo("{\"item\":\"sku-1\"}");
        assertThat(scenario.totalWeight()).isEqualTo(100);

        assertThat(scenario.assertions()).hasSize(2);
        assertThat(scenario.assertions().get(0).type()).isEqualTo(AssertionType.PERCENTILE);
        assertThat(scenario.assertions().get(0).parameter()).isEqualTo(95);
        assertThat(scenario.assertions().get(0).threshold()).isEqualTo(250);
        assertThat(scenario.assertions().get(0).operator()).isEqualTo(ComparisonOperator.LESS_THAN);
    }

    @Test
    @DisplayName("a step without an explicit name is described by its method and path")
    void derivesStepName() {
        Scenario scenario = ScenarioParser.parse(VALID);
        assertThat(scenario.steps().get(0).name()).isEqualTo("GET /api/fast");
    }

    @Test
    @DisplayName("a trailing slash on the target would produce '//api/fast'")
    void stripsTrailingSlashFromTarget() {
        Scenario scenario =
                ScenarioParser.parse(VALID.replace("http://target-service:8081", "http://t:80/"));
        assertThat(scenario.target()).isEqualTo("http://t:80");
    }

    @Test
    @DisplayName("rejects a ramp-up longer than the run itself")
    void rejectsRampUpLongerThanDuration() {
        assertThatThrownBy(() -> ScenarioParser.parse(VALID.replace("rampUp: 30s", "rampUp: 300s")))
                .isInstanceOf(ScenarioParser.InvalidScenarioException.class)
                .hasMessageContaining("rampUp");
    }

    @Test
    @DisplayName("rejects a relative target, naming the field")
    void rejectsRelativeTarget() {
        assertThatThrownBy(
                        () ->
                                ScenarioParser.parse(
                                        VALID.replace("http://target-service:8081", "target-service")))
                .isInstanceOf(ScenarioParser.InvalidScenarioException.class)
                .hasMessageContaining("target");
    }

    @Test
    @DisplayName("reports which step is at fault")
    void reportsOffendingStepIndex() {
        assertThatThrownBy(() -> ScenarioParser.parse(VALID.replace("path: /api/slow", "path: api/slow")))
                .isInstanceOf(ScenarioParser.InvalidScenarioException.class)
                .hasMessageContaining("steps[1]");
    }

    @Test
    @DisplayName("rejects a scenario with no steps")
    void rejectsEmptySteps() {
        assertThatThrownBy(
                        () ->
                                ScenarioParser.parse(
                                        """
                                        name: empty
                                        target: http://t:80
                                        duration: 10s
                                        arrivalRate: 1
                                        """))
                .isInstanceOf(ScenarioParser.InvalidScenarioException.class)
                .hasMessageContaining("steps");
    }
}
