package io.pulseforge.common.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.pulseforge.common.domain.Assertion;
import io.pulseforge.common.domain.AssertionType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class AssertionSyntaxTest {

    @Test
    @DisplayName("percentile thresholds are normalised to milliseconds")
    void normalisesPercentileUnits() {
        assertThat(AssertionSyntax.parse("p95 < 250ms").threshold()).isEqualTo(250);
        assertThat(AssertionSyntax.parse("p95 < 1s").threshold())
                .as("seconds must be converted, not taken literally")
                .isEqualTo(1000);
    }

    @Test
    @DisplayName("error rate is read as a percentage")
    void parsesErrorRate() {
        Assertion assertion = AssertionSyntax.parse("errorRate < 1%");
        assertThat(assertion.type()).isEqualTo(AssertionType.ERROR_RATE);
        assertThat(assertion.threshold()).isEqualTo(1);
        assertThat(assertion.evaluate(0.5)).isTrue();
        assertThat(assertion.evaluate(1.5)).isFalse();
    }

    @Test
    @DisplayName("throughput assertions compare in the other direction")
    void parsesThroughput() {
        Assertion assertion = AssertionSyntax.parse("throughput > 380");
        assertThat(assertion.type()).isEqualTo(AssertionType.THROUGHPUT);
        assertThat(assertion.evaluate(400)).isTrue();
        assertThat(assertion.evaluate(300)).isFalse();
    }

    @Test
    @DisplayName("round-trips back into the syntax it was written in")
    void describeRoundTrips() {
        assertThat(AssertionSyntax.parse("p99 <= 500ms").describe()).isEqualTo("p99 <= 500ms");
        assertThat(AssertionSyntax.parse("errorRate < 1%").describe()).isEqualTo("errorRate < 1%");
    }

    @ParameterizedTest
    @ValueSource(strings = {"p95 250ms", "latency < 250ms", "p95 !~ 250ms", "p150 < 1ms", ""})
    @DisplayName("refuses expressions it cannot evaluate rather than guessing")
    void rejectsUnsupportedExpressions(String expression) {
        assertThatThrownBy(() -> AssertionSyntax.parse(expression))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
