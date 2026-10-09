package nh.kafkahandson.scheduler;

import nh.kafkahandson.service.AlpacaToKafkaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
class QuoteScheduler {

    private static final Logger log = LoggerFactory.getLogger(QuoteScheduler.class);
    private final AlpacaToKafkaService service;

    QuoteScheduler(AlpacaToKafkaService service) {
        this.service = service;
    }

    @Scheduled(fixedRateString = "${app.quotes.poll-interval:PT5M}", initialDelayString = "PT10S")
    void poll() {
        try {
            service.fetchAndPublish();
        } catch (Exception e) {
            log.error("Quote poll failed", e);   // keep the schedule alive
        }
    }
}
