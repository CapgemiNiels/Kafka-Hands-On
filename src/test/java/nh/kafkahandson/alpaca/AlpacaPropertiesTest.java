package nh.kafkahandson.alpaca;

import nh.kafkahandson.market.Quote;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class AlpacaPropertiesTest {

    private static final String[] VALID = {
            "alpaca.base-url=https://data.alpaca.markets",
            "alpaca.api-key=key",
            "alpaca.api-secret=secret",
            "alpaca.feed=iex",
            "alpaca.symbols=AAPL,MSFT"
    };

    private final ApplicationContextRunner runner =
            new ApplicationContextRunner().withUserConfiguration(PropertiesConfig.class);

    @EnableConfigurationProperties(AlpacaProperties.class)
    static class PropertiesConfig {
    }

    @Test
    void bindsValidProperties() {
        runner.withPropertyValues(VALID)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    AlpacaProperties properties = context.getBean(AlpacaProperties.class);
                    assertThat(properties.baseUrl()).isEqualTo("https://data.alpaca.markets");
                    assertThat(properties.apiKey()).isEqualTo("key");
                    assertThat(properties.apiSecret()).isEqualTo("secret");
                    assertThat(properties.feed()).isEqualTo("iex");
                    assertThat(properties.symbols()).containsExactly("AAPL", "MSFT");
                });
    }

    @ParameterizedTest
    @ValueSource(strings = {"alpaca.base-url", "alpaca.api-key", "alpaca.api-secret"})
    void failsToStartWhenRequiredPropertyIsBlank(String property) {
        runner.withPropertyValues(VALID)
                .withPropertyValues(property + "=")
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void failsToStartWhenApiKeyIsMissing() {
        runner.withPropertyValues(
                        "alpaca.base-url=https://data.alpaca.markets",
                        "alpaca.api-secret=secret")
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void failsToStartWhenApiSecretIsMissing() {
        runner.withPropertyValues(
                        "alpaca.base-url=https://data.alpaca.markets",
                        "alpaca.api-key=key")
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void failsToStartWhenBaseUrlIsMissing() {
        runner.withPropertyValues("alpaca.api-key=key", "alpaca.api-secret=secret")
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void startsWithoutFeedAndSymbols() {
        runner.withPropertyValues(
                        "alpaca.base-url=https://data.alpaca.markets",
                        "alpaca.api-key=key",
                        "alpaca.api-secret=secret")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(AlpacaProperties.class).feed()).isNull();
                    assertThat(context.getBean(AlpacaProperties.class).symbols()).isNull();
                });
    }
}