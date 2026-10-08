package nh.kafkahandson.alpaca;

import jakarta.validation.constraints.NotBlank;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("alpaca")
record AlpacaProperties(
        @NotBlank String baseUrl,
        @NotBlank String apiKey,
        @NotBlank String apiSecret,
        String feed,
        List<String> symbols) {
}
