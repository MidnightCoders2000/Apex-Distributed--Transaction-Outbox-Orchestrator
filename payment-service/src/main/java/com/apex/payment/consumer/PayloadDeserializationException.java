package com.apex.payment.consumer;

public class PayloadDeserializationException extends RuntimeException {
    public PayloadDeserializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
