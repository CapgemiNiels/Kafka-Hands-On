package nh.kafkahandson.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import nh.kafkahandson.service.AlpacaToKafkaService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.scheduling.annotation.Scheduled;

@ExtendWith({MockitoExtension.class, OutputCaptureExtension.class})
class QuoteSchedulerTest {

    @Mock
    AlpacaToKafkaService service;

    @Test
    void pollTriggersFetchAndPublishOnce() {
        new QuoteScheduler(service).poll();

        verify(service, times(1)).fetchAndPublish();
    }

    @Test
    void pollDoesNotThrowWhenServiceFails() {
        doThrow(new IllegalStateException("boom")).when(service).fetchAndPublish();

        assertThatCode(() -> new QuoteScheduler(service).poll()).doesNotThrowAnyException();
    }

    @Test
    void pollLogsErrorWhenServiceFails(CapturedOutput output) {
        doThrow(new IllegalStateException("boom")).when(service).fetchAndPublish();

        new QuoteScheduler(service).poll();

        assertThat(output).contains("Quote poll failed").contains("boom");
    }

    @Test
    void nextPollRunsAgainAfterAFailedPoll() {
        doThrow(new IllegalStateException("boom")).doNothing().when(service).fetchAndPublish();
        var scheduler = new QuoteScheduler(service);

        scheduler.poll();
        scheduler.poll();

        verify(service, times(2)).fetchAndPublish();
    }

    @Test
    void pollIsScheduledWithConfigurablePollIntervalDefaultingToFiveMinutes() throws NoSuchMethodException {
        Scheduled scheduled = QuoteScheduler.class.getDeclaredMethod("poll").getAnnotation(Scheduled.class);

        assertThat(scheduled.fixedRateString()).isEqualTo("${app.quotes.poll-interval:PT5M}");
    }

    @Test
    void pollStartsAfterTenSecondInitialDelay() throws NoSuchMethodException {
        Scheduled scheduled = QuoteScheduler.class.getDeclaredMethod("poll").getAnnotation(Scheduled.class);

        assertThat(scheduled.initialDelayString()).isEqualTo("PT10S");
    }
}
