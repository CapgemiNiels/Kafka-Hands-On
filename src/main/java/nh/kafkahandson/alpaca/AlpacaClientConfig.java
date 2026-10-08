package nh.kafkahandson.alpaca;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

@Configuration
public class AlpacaClientConfig {

    @Bean
    public RestClient alpacaRestClient(RestClient.Builder builder, AlpacaProperties alpacaProperties) {
        return builder
                .baseUrl(alpacaProperties.baseUrl())
                .defaultHeader("APCA-API-KEY-ID", alpacaProperties.apiKey())
                .defaultHeader("APCA-API-SECRET-KEY", alpacaProperties.apiSecret())
                .build();
    }

}
