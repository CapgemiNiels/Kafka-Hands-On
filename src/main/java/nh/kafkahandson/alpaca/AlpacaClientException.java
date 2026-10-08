package nh.kafkahandson.alpaca;

public class AlpacaClientException extends RuntimeException {
    public AlpacaClientException(String message) {
        super(message);
    }

    public AlpacaClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
